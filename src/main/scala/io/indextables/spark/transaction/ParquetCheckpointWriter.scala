/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.transaction

import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import java.util.concurrent.TimeUnit.MINUTES

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Writer for Parquet-format checkpoint files.
 *
 * Parquet checkpoints provide improved performance for large tables through:
 *   - Columnar storage with efficient compression
 *   - Predicate pushdown for filtered reads
 *   - Native Spark integration for parallel writes
 *
 * Checkpoint files are written to `_checkpoints/` subdirectory under the transaction log path.
 *
 * @param transactionLogPath
 *   Base path for transaction log
 * @param cloudProvider
 *   Cloud storage provider for file operations
 * @param spark
 *   SparkSession for DataFrame creation and Parquet writes
 * @param options
 *   Configuration options
 */
class ParquetCheckpointWriter(
    transactionLogPath: Path,
    cloudProvider: CloudStorageProvider,
    spark: SparkSession,
    options: CaseInsensitiveStringMap) {

  private val logger = LoggerFactory.getLogger(classOf[ParquetCheckpointWriter])

  // Configuration options
  private val compressionCodec =
    Option(options.get("spark.indextables.checkpoint.parquet.compression")).getOrElse("snappy")
  private val validateOnWrite =
    options.getBoolean("spark.indextables.checkpoint.validateOnWrite", true)

  // Multi-part checkpoint configuration
  private val multiPartEnabled =
    options.getBoolean("spark.indextables.checkpoint.multipart.enabled", false)
  private val maxActionsPerPart =
    options.getInt("spark.indextables.checkpoint.multipart.maxActionsPerPart", 50000)
  private val parallelWriteEnabled =
    options.getBoolean("spark.indextables.checkpoint.multipart.parallelWrite", true)
  private val parallelWriteTimeoutMinutes =
    options.getInt("spark.indextables.checkpoint.multipart.parallelWriteTimeoutMinutes", 30)
  private val parallelWriteThreads =
    options.getInt("spark.indextables.checkpoint.multipart.parallelWriteThreads", 0)

  // Paths
  private val checkpointsDir     = new Path(transactionLogPath, "_checkpoints")
  private val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")

  /**
   * Write a Parquet checkpoint for the given version containing all actions.
   *
   * Automatically chooses between single-file and multi-part checkpoint based on
   * configuration and action count.
   *
   * @param version
   *   Transaction version to checkpoint
   * @param actions
   *   All actions to include in the checkpoint
   * @return
   *   Metadata about the written checkpoint
   */
  def writeCheckpoint(version: Long, actions: Seq[Action]): ParquetCheckpointMetadata =
    if (multiPartEnabled && actions.length > maxActionsPerPart) {
      val numParts = calculateNumParts(actions.length)
      writeMultiPartCheckpoint(version, actions, numParts)
    } else {
      writeSingleFileCheckpoint(version, actions)
    }

  /**
   * Write a single-file Parquet checkpoint for the given version.
   *
   * @param version
   *   Transaction version to checkpoint
   * @param actions
   *   All actions to include in the checkpoint
   * @return
   *   Metadata about the written checkpoint
   */
  def writeSingleFileCheckpoint(version: Long, actions: Seq[Action]): ParquetCheckpointMetadata = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Creating Parquet checkpoint for version $version with ${actions.length} actions")

    try {
      // Convert actions to ParquetCheckpointEntry
      val entries = actions.map(ParquetCheckpointEntry.fromAction)

      // Create Dataset from entries
      implicit val encoder = Encoders.product[ParquetCheckpointEntry]
      val dataset: Dataset[ParquetCheckpointEntry] = spark.createDataset(entries)

      // Get checkpoint path
      val checkpointPath    = getCheckpointPath(version)
      val checkpointPathStr = checkpointPath.toString

      // Ensure checkpoints directory exists (for local filesystem)
      ensureCheckpointDirectoryExists()

      // Write as Parquet with configured compression
      dataset.write
        .option("compression", compressionCodec)
        .mode("overwrite")
        .parquet(checkpointPathStr)

      // Get the size of the written Parquet file(s)
      val sizeInBytes = getCheckpointSize(checkpointPathStr)

      // Validate if enabled
      if (validateOnWrite) {
        validateCheckpoint(version, checkpointPathStr, actions.length)
      }

      // Create metadata (single-file checkpoint, no parts)
      val metadata = ParquetCheckpointMetadata.create(
        version = version,
        actions = actions,
        sizeInBytes = sizeInBytes,
        parts = None
      )

      // Update _last_checkpoint file
      writeLastCheckpointFile(metadata)

      val duration = System.currentTimeMillis() - startTime
      logger.info(
        s"Successfully created Parquet checkpoint at version $version " +
          s"with ${actions.length} actions, ${sizeInBytes} bytes, " +
          s"compression: $compressionCodec in ${duration}ms"
      )

      metadata
    } catch {
      case e: Exception =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(s"Failed to create Parquet checkpoint for version $version after ${duration}ms", e)
        throw e
    }
  }

  /**
   * Write a multi-part Parquet checkpoint for the given version.
   *
   * Multi-part checkpoints distribute entries across multiple Parquet files for better
   * write performance and memory management with very large checkpoints.
   *
   * @param version
   *   Transaction version to checkpoint
   * @param actions
   *   All actions to include in the checkpoint
   * @param numParts
   *   Number of parts to split the checkpoint into
   * @return
   *   Metadata about the written checkpoint
   */
  def writeMultiPartCheckpoint(
      version: Long,
      actions: Seq[Action],
      numParts: Int
  ): ParquetCheckpointMetadata = {
    val startTime = System.currentTimeMillis()
    logger.info(
      s"Creating $numParts-part Parquet checkpoint for version $version " +
        s"with ${actions.length} actions"
    )

    try {
      // Ensure checkpoints directory exists
      ensureCheckpointDirectoryExists()

      // Convert actions to entries
      val entries = actions.map(ParquetCheckpointEntry.fromAction)

      // Distribute entries across parts using round-robin
      val partitionedEntries = distributeEntriesToParts(entries, numParts)

      // Write all parts (parallel or sequential based on config)
      val partSizes = if (parallelWriteEnabled) {
        writePartsInParallel(version, partitionedEntries)
      } else {
        writePartsSequentially(version, partitionedEntries)
      }

      // Calculate total size
      val totalSizeInBytes = partSizes.sum

      // Validate if enabled
      if (validateOnWrite) {
        validateMultiPartCheckpoint(version, numParts, actions.length)
      }

      // Create metadata with parts count
      val metadata = ParquetCheckpointMetadata.create(
        version = version,
        actions = actions,
        sizeInBytes = totalSizeInBytes,
        parts = Some(numParts)
      )

      // Update _last_checkpoint file with parts info
      writeLastCheckpointFile(metadata)

      val duration = System.currentTimeMillis() - startTime
      logger.info(
        s"Successfully created $numParts-part Parquet checkpoint at version $version " +
          s"with ${actions.length} actions, ${totalSizeInBytes} bytes total, " +
          s"compression: $compressionCodec in ${duration}ms"
      )

      metadata
    } catch {
      case e: Exception =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(
          s"Failed to create multi-part Parquet checkpoint for version $version after ${duration}ms",
          e
        )
        // Attempt to clean up partial checkpoint
        cleanupPartialCheckpoint(version, numParts)
        throw e
    }
  }

  /**
   * Get the path for a checkpoint file at the given version.
   *
   * Format: _checkpoints/{version:020d}.checkpoint.parquet
   *
   * @param version
   *   Transaction version
   * @return
   *   Path to the checkpoint file
   */
  def getCheckpointPath(version: Long): Path =
    new Path(checkpointsDir, f"$version%020d.checkpoint.parquet")

  /**
   * Get the path for a specific part of a multi-part checkpoint.
   *
   * Format: _checkpoints/{version:020d}.checkpoint.{part:010d}.parquet
   *
   * @param version
   *   Transaction version
   * @param part
   *   Part number (0-indexed)
   * @return
   *   Path to the checkpoint part file
   */
  def getMultiPartCheckpointPath(version: Long, part: Int): Path =
    new Path(checkpointsDir, f"$version%020d.checkpoint.$part%010d.parquet")

  /**
   * Calculate the number of parts needed for a given action count.
   *
   * @param actionCount
   *   Total number of actions to checkpoint
   * @return
   *   Number of parts (minimum 2 for multi-part checkpoints)
   */
  def calculateNumParts(actionCount: Int): Int = {
    val parts = (actionCount + maxActionsPerPart - 1) / maxActionsPerPart
    math.max(2, parts) // Minimum 2 parts for multi-part checkpoints
  }

  /**
   * Distribute entries across parts using round-robin distribution.
   *
   * This ensures even distribution of entries across all parts, which provides
   * better load balancing during parallel reads.
   *
   * @param entries
   *   All entries to distribute
   * @param numParts
   *   Number of parts to distribute across
   * @return
   *   Sequence of entry sequences, one per part
   */
  def distributeEntriesToParts(
      entries: Seq[ParquetCheckpointEntry],
      numParts: Int
  ): Seq[Seq[ParquetCheckpointEntry]] = {
    // Initialize empty sequences for each part
    val partBuffers = Array.fill(numParts)(Seq.newBuilder[ParquetCheckpointEntry])

    // Round-robin distribution
    entries.zipWithIndex.foreach { case (entry, idx) =>
      val partIndex = idx % numParts
      partBuffers(partIndex) += entry
    }

    partBuffers.map(_.result()).toSeq
  }

  /**
   * Write a single part of a multi-part checkpoint.
   *
   * @param version
   *   Transaction version
   * @param partIndex
   *   Part index (0-indexed)
   * @param entries
   *   Entries for this part
   * @return
   *   Size in bytes of the written part
   */
  def writeSinglePart(
      version: Long,
      partIndex: Int,
      entries: Seq[ParquetCheckpointEntry]
  ): Long = {
    val partPath    = getMultiPartCheckpointPath(version, partIndex)
    val partPathStr = partPath.toString

    logger.debug(s"Writing checkpoint part $partIndex with ${entries.length} entries to $partPath")

    // Create Dataset from entries
    implicit val encoder = Encoders.product[ParquetCheckpointEntry]
    val dataset: Dataset[ParquetCheckpointEntry] = spark.createDataset(entries)

    // Write as Parquet with configured compression
    dataset.write
      .option("compression", compressionCodec)
      .mode("overwrite")
      .parquet(partPathStr)

    // Get the size of the written part
    getCheckpointSize(partPathStr)
  }

  /**
   * Write all parts sequentially.
   *
   * @param version
   *   Transaction version
   * @param partitionedEntries
   *   Entries distributed across parts
   * @return
   *   Sequence of sizes for each part
   */
  private def writePartsSequentially(
      version: Long,
      partitionedEntries: Seq[Seq[ParquetCheckpointEntry]]
  ): Seq[Long] =
    partitionedEntries.zipWithIndex.map { case (entries, partIndex) =>
      writeSinglePart(version, partIndex, entries)
    }

  /**
   * Write all parts in parallel.
   *
   * @param version
   *   Transaction version
   * @param partitionedEntries
   *   Entries distributed across parts
   * @return
   *   Sequence of sizes for each part
   * @throws RuntimeException
   *   if parallel writes exceed the configured timeout
   */
  private def writePartsInParallel(
      version: Long,
      partitionedEntries: Seq[Seq[ParquetCheckpointEntry]]
  ): Seq[Long] = {
    val numParts = partitionedEntries.length

    // Calculate thread pool size based on configuration
    val poolSize = if (parallelWriteThreads > 0) {
      math.min(parallelWriteThreads, numParts)
    } else {
      math.min(numParts, Runtime.getRuntime.availableProcessors())
    }

    // Use a fixed thread pool for parallel writes
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(
      java.util.concurrent.Executors.newFixedThreadPool(poolSize)
    )

    try {
      val futures = partitionedEntries.zipWithIndex.map { case (entries, partIndex) =>
        Future {
          writeSinglePart(version, partIndex, entries)
        }
      }

      // Wait for all writes to complete with configurable timeout
      val combinedFuture = Future.sequence(futures)
      Await.result(combinedFuture, Duration(parallelWriteTimeoutMinutes, MINUTES))
    } catch {
      case e: TimeoutException =>
        logger.error(
          s"Parallel checkpoint write timed out for version $version: " +
            s"$numParts parts did not complete within $parallelWriteTimeoutMinutes minutes"
        )
        // Clean up any partial checkpoint files
        cleanupPartialCheckpoint(version, numParts)
        throw new RuntimeException(
          s"Parallel checkpoint write for version $version timed out after " +
            s"$parallelWriteTimeoutMinutes minutes ($numParts parts). " +
            s"Consider increasing spark.indextables.checkpoint.multipart.parallelWriteTimeoutMinutes",
          e
        )
    } finally {
      ec match {
        case es: java.util.concurrent.ExecutorService =>
          es.shutdown()
          // Wait up to 10 seconds for tasks to terminate gracefully
          if (!es.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
            logger.warn(s"Executor did not terminate gracefully, forcing shutdown")
            es.shutdownNow()
          }
        case _ => // ignore
      }
    }
  }

  /**
   * Clean up partial checkpoint files after a failure.
   *
   * Attempts to delete all part files that may have been written before the failure.
   *
   * @param version
   *   Transaction version
   * @param numParts
   *   Number of parts that were planned
   */
  def cleanupPartialCheckpoint(version: Long, numParts: Int): Unit = {
    logger.warn(s"Cleaning up partial checkpoint for version $version")

    (0 until numParts).foreach { partIndex =>
      val partPath = getMultiPartCheckpointPath(version, partIndex)
      Try {
        if (cloudProvider.exists(partPath.toString)) {
          cloudProvider.deleteFile(partPath.toString)
          logger.debug(s"Deleted partial checkpoint part $partIndex at $partPath")
        }
      } match {
        case Success(_) => // OK
        case Failure(e) =>
          logger.warn(s"Failed to clean up checkpoint part $partIndex at $partPath: ${e.getMessage}")
      }
    }
  }

  /**
   * Validate a multi-part checkpoint by reading all parts and checking total entry count.
   *
   * @param version
   *   Transaction version
   * @param numParts
   *   Number of parts
   * @param expectedCount
   *   Expected total number of entries
   */
  def validateMultiPartCheckpoint(version: Long, numParts: Int, expectedCount: Int): Unit = {
    logger.debug(s"Validating $numParts-part Parquet checkpoint at version $version")

    try {
      // Build paths for all parts
      val partPaths = (0 until numParts).map { part =>
        getMultiPartCheckpointPath(version, part).toString
      }

      // Verify all parts exist
      val missingParts = partPaths.zipWithIndex.filterNot { case (path, _) =>
        cloudProvider.exists(path)
      }

      if (missingParts.nonEmpty) {
        val missingIndices = missingParts.map(_._2).mkString(", ")
        throw new IllegalStateException(
          s"Multi-part checkpoint validation failed: missing parts [$missingIndices]"
        )
      }

      // Read all parts and count entries
      val df        = spark.read.parquet(partPaths: _*)
      val readCount = df.count()

      if (readCount != expectedCount) {
        throw new IllegalStateException(
          s"Multi-part checkpoint validation failed: " +
            s"expected $expectedCount entries but found $readCount"
        )
      }

      logger.debug(
        s"Multi-part checkpoint validation passed: " +
          s"$readCount entries across $numParts parts at version $version"
      )
    } catch {
      case e: IllegalStateException =>
        throw e
      case e: Exception =>
        throw new IllegalStateException(
          s"Multi-part checkpoint validation failed at version $version: ${e.getMessage}",
          e
        )
    }
  }

  /**
   * Write the _last_checkpoint file with metadata about the latest checkpoint.
   *
   * @param metadata
   *   Checkpoint metadata to write
   */
  def writeLastCheckpointFile(metadata: ParquetCheckpointMetadata): Unit = {
    logger.debug(s"Writing _last_checkpoint file for version ${metadata.version}")

    try {
      val lastCheckpointInfo = metadata.toLastCheckpointFileInfo
      val json               = JsonUtil.mapper.writeValueAsString(lastCheckpointInfo)

      cloudProvider.writeFile(lastCheckpointPath.toString, json.getBytes("UTF-8"))

      logger.debug(s"Successfully wrote _last_checkpoint file for version ${metadata.version}")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to write _last_checkpoint file for version ${metadata.version}", e)
        throw e
    }
  }

  /**
   * Ensure the _checkpoints directory exists.
   */
  private def ensureCheckpointDirectoryExists(): Unit =
    Try {
      cloudProvider.createDirectory(checkpointsDir.toString)
    } match {
      case Success(_) =>
        logger.debug(s"Ensured checkpoint directory exists: ${checkpointsDir.toString}")
      case Failure(e) =>
        // Directory might already exist or creation is not needed for cloud storage
        logger.debug(s"Could not create checkpoint directory (may already exist): ${e.getMessage}")
    }

  /**
   * Get the total size of the checkpoint file(s) at the given path.
   *
   * Since Spark writes Parquet files to a directory, we need to sum up all part files.
   *
   * @param checkpointPath
   *   Path to the checkpoint (directory)
   * @return
   *   Total size in bytes
   */
  private def getCheckpointSize(checkpointPath: String): Long =
    Try {
      val files = cloudProvider.listFiles(checkpointPath, recursive = true)
      val totalSize = files
        .filterNot(_.isDirectory)
        .filterNot(_.path.contains("_SUCCESS"))
        .filterNot(_.path.contains("_committed"))
        .filterNot(_.path.contains("_started"))
        .map(_.size)
        .sum
      totalSize
    } match {
      case Success(size) => size
      case Failure(e) =>
        logger.warn(s"Could not determine checkpoint size at $checkpointPath: ${e.getMessage}")
        0L
    }

  /**
   * Validate the written checkpoint by reading it back and checking entry count.
   *
   * @param version
   *   Expected version
   * @param checkpointPath
   *   Path to the checkpoint
   * @param expectedCount
   *   Expected number of entries
   */
  private def validateCheckpoint(version: Long, checkpointPath: String, expectedCount: Int): Unit = {
    logger.debug(s"Validating Parquet checkpoint at version $version")

    try {
      val readDf    = spark.read.parquet(checkpointPath)
      val readCount = readDf.count()

      if (readCount != expectedCount) {
        throw new IllegalStateException(
          s"Checkpoint validation failed: expected $expectedCount entries but found $readCount"
        )
      }

      logger.debug(s"Checkpoint validation passed: $readCount entries at version $version")
    } catch {
      case e: IllegalStateException =>
        throw e
      case e: Exception =>
        throw new IllegalStateException(s"Checkpoint validation failed at version $version: ${e.getMessage}", e)
    }
  }
}

object ParquetCheckpointWriter {

  /**
   * Default compression codec for Parquet checkpoints.
   */
  val DEFAULT_COMPRESSION = "snappy"

  /**
   * Configuration key for Parquet compression codec.
   */
  val COMPRESSION_KEY = "spark.indextables.checkpoint.parquet.compression"

  /**
   * Configuration key for validation on write.
   */
  val VALIDATE_ON_WRITE_KEY = "spark.indextables.checkpoint.validateOnWrite"

  /**
   * Subdirectory name for Parquet checkpoints.
   */
  val CHECKPOINTS_DIR = "_checkpoints"

  // Multi-part checkpoint configuration keys

  /**
   * Configuration key to enable multi-part checkpoints.
   */
  val MULTIPART_ENABLED_KEY = "spark.indextables.checkpoint.multipart.enabled"

  /**
   * Configuration key for maximum actions per checkpoint part.
   */
  val MULTIPART_MAX_ACTIONS_PER_PART_KEY = "spark.indextables.checkpoint.multipart.maxActionsPerPart"

  /**
   * Configuration key to enable parallel writes for multi-part checkpoints.
   */
  val MULTIPART_PARALLEL_WRITE_KEY = "spark.indextables.checkpoint.multipart.parallelWrite"

  /**
   * Configuration key for parallel write timeout in minutes.
   */
  val MULTIPART_PARALLEL_WRITE_TIMEOUT_KEY =
    "spark.indextables.checkpoint.multipart.parallelWriteTimeoutMinutes"

  /**
   * Configuration key for parallel write thread pool size.
   * When 0, uses auto = min(numParts, availableProcessors).
   * When > 0, uses the specified value (capped at numParts).
   */
  val MULTIPART_PARALLEL_WRITE_THREADS_KEY =
    "spark.indextables.checkpoint.multipart.parallelWriteThreads"

  /**
   * Default value for multi-part enabled.
   */
  val DEFAULT_MULTIPART_ENABLED = false

  /**
   * Default maximum actions per checkpoint part.
   */
  val DEFAULT_MAX_ACTIONS_PER_PART = 50000

  /**
   * Default value for parallel write enabled.
   */
  val DEFAULT_PARALLEL_WRITE_ENABLED = true

  /**
   * Default parallel write timeout in minutes.
   */
  val DEFAULT_PARALLEL_WRITE_TIMEOUT_MINUTES = 30

  /**
   * Default parallel write thread pool size.
   * 0 means auto = min(numParts, availableProcessors).
   */
  val DEFAULT_PARALLEL_WRITE_THREADS = 0

  /**
   * Create a ParquetCheckpointWriter for a transaction log path.
   *
   * @param transactionLogPath
   *   Base path for transaction log
   * @param cloudProvider
   *   Cloud storage provider
   * @param spark
   *   SparkSession
   * @param options
   *   Configuration options
   * @return
   *   New ParquetCheckpointWriter instance
   */
  def apply(
      transactionLogPath: Path,
      cloudProvider: CloudStorageProvider,
      spark: SparkSession,
      options: CaseInsensitiveStringMap
  ): ParquetCheckpointWriter =
    new ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

  /**
   * Create a ParquetCheckpointWriter with default empty options.
   *
   * @param transactionLogPath
   *   Base path for transaction log
   * @param cloudProvider
   *   Cloud storage provider
   * @param spark
   *   SparkSession
   * @return
   *   New ParquetCheckpointWriter instance
   */
  def apply(
      transactionLogPath: Path,
      cloudProvider: CloudStorageProvider,
      spark: SparkSession
  ): ParquetCheckpointWriter = {
    import scala.jdk.CollectionConverters._
    val emptyOptions = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    new ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, emptyOptions)
  }
}
