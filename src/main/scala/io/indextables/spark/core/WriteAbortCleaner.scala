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

package io.indextables.spark.core

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.indextables.spark.io.{CloudStorageProvider, CloudStorageProviderFactory}
import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

/**
 * Result of write abort cleanup operation.
 *
 * @param deletedCount
 *   Number of files successfully deleted
 * @param failedCount
 *   Number of files that failed to delete
 * @param totalBytes
 *   Total bytes deleted (if available)
 */
case class WriteAbortCleanupResult(
  deletedCount: Long,
  failedCount: Long,
  totalBytes: Long = 0L)

/**
 * Cleans up uncommitted files when a write operation is aborted.
 *
 * This class handles the deletion of split files that were created during a write operation
 * but were not committed to the transaction log due to an abort. It uses CloudStorageProvider
 * for multi-cloud support (S3, Azure, local, HDFS) and includes retry logic with exponential
 * backoff for transient failures.
 *
 * Design follows patterns from PurgeOrphanedSplitsExecutor for consistency.
 */
object WriteAbortCleaner {

  private val logger = LoggerFactory.getLogger(WriteAbortCleaner.getClass)

  /** Default number of retry attempts for file deletion */
  private val DefaultMaxRetries = 3

  /** Base delay in milliseconds for exponential backoff */
  private val BaseBackoffDelayMs = 100

  /**
   * Clean up uncommitted files from a failed write operation.
   *
   * This method attempts to delete all files referenced in the provided AddActions.
   * It uses retry logic with exponential backoff and handles all errors gracefully
   * without throwing exceptions.
   *
   * @param addActions
   *   The AddActions containing paths to files that need to be deleted
   * @param tablePath
   *   The base path of the table
   * @param options
   *   Configuration options (may contain cloud credentials)
   * @param hadoopConf
   *   Hadoop configuration
   * @return
   *   WriteAbortCleanupResult with counts of deleted and failed files
   */
  def cleanupUncommittedFiles(
    addActions: Seq[AddAction],
    tablePath: Path,
    options: Map[String, String],
    hadoopConf: Configuration
  ): WriteAbortCleanupResult =
    try {
      if (addActions.isEmpty) {
        logger.info("No uncommitted files to clean up during abort")
        return WriteAbortCleanupResult(0L, 0L, 0L)
      }

      logger.info(s"Starting cleanup of ${addActions.length} uncommitted files during write abort")

      // Convert options to CaseInsensitiveStringMap for CloudStorageProvider
      val optionsMap = new java.util.HashMap[String, String]()
      options.foreach { case (k, v) => optionsMap.put(k, v) }
      val configOptions = new CaseInsensitiveStringMap(optionsMap)

      // Create CloudStorageProvider for file deletion
      val provider = CloudStorageProviderFactory.createProvider(
        tablePath.toString,
        configOptions,
        hadoopConf
      )

      try {
        var deletedCount = 0L
        var failedCount  = 0L
        var totalBytes   = 0L

        addActions.foreach { addAction =>
          val filePath = resolveFilePath(addAction.path, tablePath)
          val fileSize = addAction.size

          val deleted = deleteFileWithRetry(provider, filePath, DefaultMaxRetries)

          if (deleted) {
            deletedCount += 1
            totalBytes += fileSize
            logger.info(s"Deleted uncommitted file: $filePath")
          } else {
            failedCount += 1
            logger.warn(s"Failed to delete uncommitted file after $DefaultMaxRetries attempts: $filePath")
          }
        }

        val result = WriteAbortCleanupResult(deletedCount, failedCount, totalBytes)

        if (failedCount == 0) {
          logger.info(s"Successfully cleaned up $deletedCount uncommitted files ($totalBytes bytes)")
        } else {
          logger.warn(s"Abort cleanup completed with issues: $deletedCount deleted, $failedCount failed")
        }

        result
      } finally
        provider.close()

    } catch {
      case e: Exception =>
        // Graceful degradation - log the error but don't throw
        logger.warn(s"Failed to clean up uncommitted files during abort: ${e.getMessage}", e)
        WriteAbortCleanupResult(0L, addActions.length.toLong, 0L)
    }

  /**
   * Resolve the full file path from an AddAction path.
   *
   * AddAction paths may be relative (just filename) or absolute. This method
   * ensures we have a full path for deletion.
   *
   * @param actionPath
   *   The path from the AddAction
   * @param tablePath
   *   The base table path
   * @return
   *   The resolved absolute path
   */
  private def resolveFilePath(actionPath: String, tablePath: Path): String =
    if (actionPath.startsWith("/") || actionPath.contains("://")) {
      // Already an absolute path or URI
      actionPath
    } else {
      // Relative path - combine with table path
      new Path(tablePath, actionPath).toString
    }

  /**
   * Delete a file with retry logic and exponential backoff.
   *
   * This method follows the retry pattern from PurgeOrphanedSplitsExecutor,
   * with exponential backoff between attempts.
   *
   * @param provider
   *   The CloudStorageProvider to use for deletion
   * @param filePath
   *   The path to the file to delete
   * @param maxRetries
   *   Maximum number of retry attempts
   * @return
   *   true if the file was deleted (or already didn't exist), false otherwise
   */
  private def deleteFileWithRetry(
    provider: CloudStorageProvider,
    filePath: String,
    maxRetries: Int
  ): Boolean = {
    var attempt  = 0
    var deleted  = false
    var retrying = true

    while (retrying && attempt < maxRetries)
      try {
        deleted = provider.deleteFile(filePath)
        retrying = false
      } catch {
        case _: java.io.FileNotFoundException =>
          // File already doesn't exist - consider this a success
          logger.debug(s"File already deleted or doesn't exist: $filePath")
          deleted = true
          retrying = false
        case e: Exception if attempt < maxRetries - 1 =>
          // Transient error - retry with exponential backoff
          val backoffMs = BaseBackoffDelayMs * (attempt + 1)
          logger.debug(s"Retry ${attempt + 1}/$maxRetries for deleting $filePath after ${e.getMessage}, waiting ${backoffMs}ms")
          Thread.sleep(backoffMs)
          attempt += 1
        case e: Exception =>
          // Final attempt failed
          logger.warn(s"Final retry failed for deleting $filePath: ${e.getMessage}")
          deleted = false
          retrying = false
      }

    deleted
  }

  /**
   * Clean up uncommitted files using serialized options.
   *
   * Convenience overload that accepts serialized hadoop config as a Map,
   * which is useful when calling from serializable write classes.
   *
   * @param addActions
   *   The AddActions containing paths to files that need to be deleted
   * @param tablePath
   *   The base path of the table
   * @param serializedOptions
   *   Serialized options map
   * @param serializedHadoopConf
   *   Serialized hadoop configuration as a Map
   * @return
   *   WriteAbortCleanupResult with counts of deleted and failed files
   */
  def cleanupUncommittedFiles(
    addActions: Seq[AddAction],
    tablePath: Path,
    serializedOptions: Map[String, String],
    serializedHadoopConf: Map[String, String]
  ): WriteAbortCleanupResult = {
    // Reconstruct Hadoop configuration from serialized map
    val hadoopConf = new Configuration()
    serializedHadoopConf.foreach { case (k, v) => hadoopConf.set(k, v) }

    // Merge serialized options with hadoop conf options for cloud credentials
    val combinedOptions = serializedHadoopConf ++ serializedOptions

    cleanupUncommittedFiles(addActions, tablePath, combinedOptions, hadoopConf)
  }
}
