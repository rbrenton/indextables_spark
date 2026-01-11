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

import scala.util.{Failure, Success, Try}

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

  // Paths
  private val checkpointsDir     = new Path(transactionLogPath, "_checkpoints")
  private val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")

  /**
   * Write a Parquet checkpoint for the given version containing all actions.
   *
   * @param version
   *   Transaction version to checkpoint
   * @param actions
   *   All actions to include in the checkpoint
   * @return
   *   Metadata about the written checkpoint
   */
  def writeCheckpoint(version: Long, actions: Seq[Action]): ParquetCheckpointMetadata = {
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

      // Create metadata
      val metadata = ParquetCheckpointMetadata.create(
        version = version,
        actions = actions,
        sizeInBytes = sizeInBytes
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
