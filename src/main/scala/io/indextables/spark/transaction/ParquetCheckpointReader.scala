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
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Reader for Parquet-format checkpoint files.
 *
 * Provides efficient reading of Parquet checkpoints using Spark's built-in Parquet support
 * with optimizations for:
 *   - Column pruning: Only reads columns needed for the specific operation
 *   - Predicate pushdown: Filters by actionType at storage level
 *   - Multi-part checkpoint support: Reads all parts when specified
 *
 * Checkpoint files are stored in `_checkpoints/{version:020d}.checkpoint.parquet` format.
 *
 * @param transactionLogPath
 *   Base path for transaction log directory
 * @param cloudProvider
 *   Cloud storage provider for file operations
 * @param spark
 *   SparkSession for DataFrame operations
 * @param options
 *   Configuration options
 */
class ParquetCheckpointReader(
    transactionLogPath: Path,
    cloudProvider: CloudStorageProvider,
    spark: SparkSession,
    options: CaseInsensitiveStringMap) {

  private val logger = LoggerFactory.getLogger(classOf[ParquetCheckpointReader])

  // Checkpoint directory path
  private val checkpointsDir = new Path(transactionLogPath, "_checkpoints")

  // Last checkpoint file path (shared with JSON format)
  private val LAST_CHECKPOINT = new Path(transactionLogPath, "_last_checkpoint")

  // Columns needed for AddAction reconstruction (optimized read)
  private val addActionColumns: Seq[String] = Seq(
    "actionType",
    "path",
    "partitionValuesJson",
    "size",
    "modificationTime",
    "dataChange",
    "stats",
    "tagsJson",
    "minValuesJson",
    "maxValuesJson",
    "numRecords",
    "footerStartOffset",
    "footerEndOffset",
    "hotcacheStartOffset",
    "hotcacheLength",
    "hasFooterOffsets",
    "timeRangeStart",
    "timeRangeEnd",
    "splitTagsJson",
    "deleteOpstamp",
    "numMergeOps",
    "docMappingJson",
    "uncompressedSizeBytes"
  )

  /**
   * Get all actions from the latest Parquet checkpoint.
   *
   * Reads the _last_checkpoint file to determine the latest version, then reads all actions from
   * that checkpoint file.
   *
   * @return
   *   Some(Seq[Action]) if checkpoint exists and is Parquet format, None otherwise
   */
  def getActionsFromCheckpoint(): Option[Seq[Action]] =
    getLastCheckpointInfo().flatMap { info =>
      if (info.isParquetFormat) {
        readParquetCheckpoint(info.version)
      } else {
        logger.debug(s"Last checkpoint is not Parquet format (format=${info.format})")
        None
      }
    }

  /**
   * Read all actions from a specific Parquet checkpoint version.
   *
   * @param version
   *   The checkpoint version to read
   * @return
   *   Some(Seq[Action]) if successful, None if checkpoint doesn't exist or on error
   */
  def readParquetCheckpoint(version: Long): Option[Seq[Action]] = {
    val startTime = System.currentTimeMillis()
    val checkpointPath = getCheckpointPath(version)

    Try {
      if (!checkpointExists(checkpointPath)) {
        logger.warn(s"Parquet checkpoint file does not exist: $checkpointPath")
        return None
      }

      logger.debug(s"Reading Parquet checkpoint from: $checkpointPath")

      // Read all entries from Parquet file
      val df = spark.read.parquet(checkpointPath.toString)
      val entries = convertDataFrameToEntries(df)
      val actions = entries.map(ParquetCheckpointEntry.toAction)

      val duration = System.currentTimeMillis() - startTime
      logger.debug(s"Read ${actions.length} actions from Parquet checkpoint v$version in ${duration}ms")

      actions
    } match {
      case Success(actions) => Some(actions)
      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        logger.warn(s"Failed to read Parquet checkpoint v$version after ${duration}ms: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Get AddActions from the latest Parquet checkpoint (optimized).
   *
   * Uses column pruning and predicate pushdown to efficiently read only AddActions. This is
   * optimized for file listing operations where only AddActions are needed.
   *
   * @return
   *   Some(Seq[AddAction]) if checkpoint exists and is Parquet format, None otherwise
   */
  def getAddActionsFromCheckpoint(): Option[Seq[AddAction]] =
    getLastCheckpointInfo().flatMap { info =>
      if (info.isParquetFormat) {
        readAddActionsFromCheckpoint(info.version)
      } else {
        logger.debug(s"Last checkpoint is not Parquet format for AddAction read")
        None
      }
    }

  /**
   * Read AddActions from a specific Parquet checkpoint version (optimized).
   *
   * Optimizations applied:
   *   - Column pruning: Only reads columns needed for AddAction
   *   - Predicate pushdown: Filters actionType === "add" at storage level
   *
   * @param version
   *   The checkpoint version to read
   * @return
   *   Some(Seq[AddAction]) if successful, None if checkpoint doesn't exist or on error
   */
  def readAddActionsFromCheckpoint(version: Long): Option[Seq[AddAction]] = {
    val startTime = System.currentTimeMillis()
    val checkpointPath = getCheckpointPath(version)

    Try {
      if (!checkpointExists(checkpointPath)) {
        logger.warn(s"Parquet checkpoint file does not exist for AddAction read: $checkpointPath")
        return None
      }

      logger.debug(s"Reading AddActions from Parquet checkpoint: $checkpointPath")

      // Read with column pruning and predicate pushdown
      val df = spark.read.parquet(checkpointPath.toString)
        .select(addActionColumns.map(col): _*)
        .filter(col("actionType") === ParquetCheckpointEntry.ACTION_TYPE_ADD)

      val entries = convertDataFrameToEntries(df)
      val addActions = entries.map(entry => ParquetCheckpointEntry.toAction(entry).asInstanceOf[AddAction])

      val duration = System.currentTimeMillis() - startTime
      logger.debug(s"Read ${addActions.length} AddActions from Parquet checkpoint v$version in ${duration}ms")

      addActions
    } match {
      case Success(actions) => Some(actions)
      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        logger.warn(s"Failed to read AddActions from Parquet checkpoint v$version after ${duration}ms: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Get the last checkpoint info from _last_checkpoint file.
   *
   * Supports both legacy LastCheckpointInfo (JSON format) and extended LastCheckpointFileInfo
   * (Parquet format with format field).
   *
   * @return
   *   Some(LastCheckpointFileInfo) if file exists and is readable, None otherwise
   */
  def getLastCheckpointInfo(): Option[LastCheckpointFileInfo] = {
    val lastCheckpointPath = LAST_CHECKPOINT.toString

    if (!cloudProvider.exists(lastCheckpointPath)) {
      logger.debug("No _last_checkpoint file found")
      return None
    }

    Try {
      val content = new String(cloudProvider.readFile(lastCheckpointPath), "UTF-8")

      // Try to parse as extended format first (with format field)
      Try(JsonUtil.mapper.readValue(content, classOf[LastCheckpointFileInfo])) match {
        case Success(info) => info
        case Failure(_) =>
          // Fall back to legacy format
          val legacyInfo = JsonUtil.mapper.readValue(content, classOf[LastCheckpointInfo])
          LastCheckpointFileInfo.fromLegacy(legacyInfo)
      }
    } match {
      case Success(info) =>
        logger.debug(s"Read last checkpoint info: version=${info.version}, format=${info.format}")
        Some(info)
      case Failure(e) =>
        logger.warn(s"Failed to read _last_checkpoint file: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Get the version number of the last checkpoint.
   *
   * @return
   *   Some(version) if checkpoint exists, None otherwise
   */
  def getLastCheckpointVersion(): Option[Long] =
    getLastCheckpointInfo().map(_.version)

  /**
   * Check if a Parquet checkpoint exists for the given version.
   *
   * @param version
   *   The checkpoint version to check
   * @return
   *   true if Parquet checkpoint exists, false otherwise
   */
  def checkpointExistsForVersion(version: Long): Boolean = {
    val checkpointPath = getCheckpointPath(version)
    checkpointExists(checkpointPath)
  }

  /**
   * Read actions filtered by type from a checkpoint.
   *
   * @param version
   *   The checkpoint version
   * @param actionTypes
   *   Set of action types to include (e.g., "add", "remove")
   * @return
   *   Some(Seq[Action]) if successful, None otherwise
   */
  def readActionsOfTypes(version: Long, actionTypes: Set[String]): Option[Seq[Action]] = {
    val startTime = System.currentTimeMillis()
    val checkpointPath = getCheckpointPath(version)

    Try {
      if (!checkpointExists(checkpointPath)) {
        logger.warn(s"Parquet checkpoint file does not exist for filtered read: $checkpointPath")
        return None
      }

      logger.debug(s"Reading actions of types ${actionTypes.mkString(", ")} from: $checkpointPath")

      // Build filter expression for action types
      val df = spark.read.parquet(checkpointPath.toString)
        .filter(col("actionType").isin(actionTypes.toSeq: _*))

      val entries = convertDataFrameToEntries(df)
      val actions = entries.map(ParquetCheckpointEntry.toAction)

      val duration = System.currentTimeMillis() - startTime
      logger.debug(s"Read ${actions.length} filtered actions from Parquet checkpoint v$version in ${duration}ms")

      actions
    } match {
      case Success(actions) => Some(actions)
      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        logger.warn(s"Failed to read filtered actions from checkpoint v$version after ${duration}ms: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Get the checkpoint file path for a version.
   *
   * Format: _checkpoints/{version:020d}.checkpoint.parquet
   *
   * @param version
   *   The checkpoint version
   * @return
   *   Path to the checkpoint file
   */
  private def getCheckpointPath(version: Long): Path =
    new Path(checkpointsDir, f"$version%020d.checkpoint.parquet")

  /**
   * Get multi-part checkpoint file path.
   *
   * Format: _checkpoints/{version:020d}.checkpoint.{part:010d}.parquet
   *
   * @param version
   *   The checkpoint version
   * @param part
   *   The part number (0-indexed)
   * @return
   *   Path to the checkpoint part file
   */
  private def getMultiPartCheckpointPath(version: Long, part: Int): Path =
    new Path(checkpointsDir, f"$version%020d.checkpoint.$part%010d.parquet")

  /**
   * Check if a checkpoint file or directory exists.
   *
   * @param path
   *   Path to check
   * @return
   *   true if exists, false otherwise
   */
  private def checkpointExists(path: Path): Boolean =
    try {
      cloudProvider.exists(path.toString)
    } catch {
      case e: Exception =>
        logger.warn(s"Error checking checkpoint existence at $path: ${e.getMessage}")
        false
    }

  /**
   * Convert a DataFrame of ParquetCheckpointEntry to a sequence of entries.
   *
   * Uses Spark's Encoder for efficient conversion.
   *
   * @param df
   *   DataFrame with ParquetCheckpointEntry schema
   * @return
   *   Sequence of ParquetCheckpointEntry objects
   */
  private def convertDataFrameToEntries(df: DataFrame): Seq[ParquetCheckpointEntry] = {
    implicit val encoder = Encoders.product[ParquetCheckpointEntry]
    df.as[ParquetCheckpointEntry].collect().toSeq
  }

  /**
   * Read a multi-part Parquet checkpoint.
   *
   * Reads all parts and combines them into a single sequence of actions.
   *
   * @param version
   *   The checkpoint version
   * @param numParts
   *   Number of parts
   * @return
   *   Some(Seq[Action]) if successful, None otherwise
   */
  def readMultiPartCheckpoint(version: Long, numParts: Int): Option[Seq[Action]] = {
    val startTime = System.currentTimeMillis()

    Try {
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
        logger.warn(s"Missing checkpoint parts for version $version: parts [$missingIndices]")
        return None
      }

      logger.debug(s"Reading $numParts-part Parquet checkpoint for version $version")

      // Read all parts and combine
      val df = spark.read.parquet(partPaths: _*)
      val entries = convertDataFrameToEntries(df)
      val actions = entries.map(ParquetCheckpointEntry.toAction)

      val duration = System.currentTimeMillis() - startTime
      logger.debug(s"Read ${actions.length} actions from $numParts-part checkpoint v$version in ${duration}ms")

      actions
    } match {
      case Success(actions) => Some(actions)
      case Failure(e) =>
        val duration = System.currentTimeMillis() - startTime
        logger.warn(s"Failed to read multi-part checkpoint v$version after ${duration}ms: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Read checkpoint based on LastCheckpointFileInfo.
   *
   * Automatically handles both single-file and multi-part checkpoints based on the info provided.
   *
   * @param info
   *   Checkpoint info from _last_checkpoint file
   * @return
   *   Some(Seq[Action]) if successful, None otherwise
   */
  def readCheckpointFromInfo(info: LastCheckpointFileInfo): Option[Seq[Action]] =
    if (!info.isParquetFormat) {
      logger.debug(s"Checkpoint format is not Parquet: ${info.format}")
      None
    } else if (info.isMultiPart) {
      readMultiPartCheckpoint(info.version, info.parts.get)
    } else {
      readParquetCheckpoint(info.version)
    }
}

object ParquetCheckpointReader {

  /**
   * Create a ParquetCheckpointReader for a transaction log path.
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
   *   New ParquetCheckpointReader instance
   */
  def apply(
      transactionLogPath: Path,
      cloudProvider: CloudStorageProvider,
      spark: SparkSession,
      options: CaseInsensitiveStringMap
  ): ParquetCheckpointReader =
    new ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

  /**
   * Create a ParquetCheckpointReader with default empty options.
   *
   * @param transactionLogPath
   *   Base path for transaction log
   * @param cloudProvider
   *   Cloud storage provider
   * @param spark
   *   SparkSession
   * @return
   *   New ParquetCheckpointReader instance
   */
  def apply(
      transactionLogPath: Path,
      cloudProvider: CloudStorageProvider,
      spark: SparkSession
  ): ParquetCheckpointReader = {
    import scala.jdk.CollectionConverters._
    val emptyOptions = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    new ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, emptyOptions)
  }
}
