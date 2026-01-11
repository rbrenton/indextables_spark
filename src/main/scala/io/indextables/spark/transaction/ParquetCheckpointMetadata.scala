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

/**
 * Metadata for a Parquet-format checkpoint file.
 *
 * Parquet checkpoints provide improved performance for large tables through:
 * - Columnar storage with efficient compression
 * - Predicate pushdown for filtered reads
 * - Support for multi-part checkpoints
 *
 * @param version      Transaction version this checkpoint represents
 * @param size         Number of entries (actions) in the checkpoint
 * @param sizeInBytes  Size of the Parquet file in bytes
 * @param numFiles     Number of AddActions (active files) in the checkpoint
 * @param createdTime  Checkpoint creation timestamp in milliseconds since epoch
 * @param format       Checkpoint format identifier ("parquet")
 * @param parts        Number of parts for multi-part checkpoints (None for single-file)
 * @param checksum     Optional SHA-256 checksum of the checkpoint file(s)
 */
case class ParquetCheckpointMetadata(
    version: Long,
    size: Long,
    sizeInBytes: Long,
    numFiles: Long,
    createdTime: Long,
    format: String = ParquetCheckpointMetadata.FORMAT_PARQUET,
    parts: Option[Int] = None,
    checksum: Option[String] = None
) {

  /**
   * Returns true if this is a multi-part checkpoint.
   */
  def isMultiPart: Boolean = parts.exists(_ > 1)

  /**
   * Converts to LastCheckpointFileInfo for writing to _last_checkpoint file.
   */
  def toLastCheckpointFileInfo: LastCheckpointFileInfo =
    LastCheckpointFileInfo(
      version = version,
      size = size,
      sizeInBytes = sizeInBytes,
      numFiles = numFiles,
      createdTime = createdTime,
      format = Some(format),
      parts = parts,
      checksum = checksum
    )
}

object ParquetCheckpointMetadata {
  val FORMAT_PARQUET = "parquet"
  val FORMAT_JSON = "json"

  /**
   * Creates ParquetCheckpointMetadata from checkpoint creation parameters.
   *
   * @param version     Transaction version
   * @param actions     All actions in the checkpoint
   * @param sizeInBytes Size of the written Parquet file
   * @param parts       Number of parts (None for single-file checkpoint)
   * @param checksum    Optional checksum of the file
   */
  def create(
      version: Long,
      actions: Seq[Action],
      sizeInBytes: Long,
      parts: Option[Int] = None,
      checksum: Option[String] = None
  ): ParquetCheckpointMetadata = {
    val numFiles = actions.count(_.isInstanceOf[AddAction])
    ParquetCheckpointMetadata(
      version = version,
      size = actions.length,
      sizeInBytes = sizeInBytes,
      numFiles = numFiles,
      createdTime = System.currentTimeMillis(),
      format = FORMAT_PARQUET,
      parts = parts,
      checksum = checksum
    )
  }

  /**
   * Creates ParquetCheckpointMetadata from an existing CheckpointInfo.
   * Used when upgrading from JSON to Parquet checkpoint format.
   */
  def fromCheckpointInfo(
      info: CheckpointInfo,
      sizeInBytes: Long,
      parts: Option[Int] = None,
      checksum: Option[String] = None
  ): ParquetCheckpointMetadata =
    ParquetCheckpointMetadata(
      version = info.version,
      size = info.size,
      sizeInBytes = sizeInBytes,
      numFiles = info.numFiles,
      createdTime = info.createdTime,
      format = FORMAT_PARQUET,
      parts = parts,
      checksum = checksum
    )
}

/**
 * Extended checkpoint file info that supports both JSON and Parquet formats.
 *
 * This class is written to the _last_checkpoint file and allows readers to
 * determine which format to use when reading the checkpoint.
 *
 * For backwards compatibility:
 * - format = None or Some("json") indicates legacy JSON format
 * - format = Some("parquet") indicates Parquet format
 *
 * @param version      Transaction version this checkpoint represents
 * @param size         Number of entries (actions) in the checkpoint
 * @param sizeInBytes  Size of the checkpoint file in bytes
 * @param numFiles     Number of AddActions (active files) in the checkpoint
 * @param createdTime  Checkpoint creation timestamp in milliseconds since epoch
 * @param format       Checkpoint format: None = legacy JSON, Some("parquet") = Parquet
 * @param parts        Number of parts for multi-part checkpoints (Parquet only)
 * @param checksum     Optional SHA-256 checksum of the checkpoint file(s)
 */
case class LastCheckpointFileInfo(
    version: Long,
    size: Long,
    sizeInBytes: Long,
    numFiles: Long,
    createdTime: Long,
    format: Option[String] = None,
    parts: Option[Int] = None,
    checksum: Option[String] = None
) {

  /**
   * Returns true if this checkpoint uses Parquet format.
   */
  def isParquetFormat: Boolean = format.contains(ParquetCheckpointMetadata.FORMAT_PARQUET)

  /**
   * Returns true if this checkpoint uses legacy JSON format.
   */
  def isJsonFormat: Boolean = format.isEmpty || format.contains(ParquetCheckpointMetadata.FORMAT_JSON)

  /**
   * Returns true if this is a multi-part checkpoint (Parquet only).
   */
  def isMultiPart: Boolean = parts.exists(_ > 1)

  /**
   * Converts to the legacy LastCheckpointInfo format.
   * Useful for backwards compatibility when writing to older systems.
   */
  def toLegacyLastCheckpointInfo: LastCheckpointInfo =
    LastCheckpointInfo(
      version = version,
      size = size,
      sizeInBytes = sizeInBytes,
      numFiles = numFiles,
      createdTime = createdTime
    )

  /**
   * Converts to CheckpointInfo for internal use.
   */
  def toCheckpointInfo: CheckpointInfo =
    CheckpointInfo(
      version = version,
      size = size,
      sizeInBytes = sizeInBytes,
      numFiles = numFiles,
      createdTime = createdTime
    )
}

object LastCheckpointFileInfo {

  /**
   * Creates from legacy LastCheckpointInfo (JSON format assumed).
   */
  def fromLegacy(info: LastCheckpointInfo): LastCheckpointFileInfo =
    LastCheckpointFileInfo(
      version = info.version,
      size = info.size,
      sizeInBytes = info.sizeInBytes,
      numFiles = info.numFiles,
      createdTime = info.createdTime,
      format = None, // Legacy format
      parts = None,
      checksum = None
    )

  /**
   * Creates from ParquetCheckpointMetadata.
   */
  def fromParquetMetadata(metadata: ParquetCheckpointMetadata): LastCheckpointFileInfo =
    metadata.toLastCheckpointFileInfo

  /**
   * Creates a new JSON format checkpoint info.
   */
  def createJsonFormat(
      version: Long,
      size: Long,
      sizeInBytes: Long,
      numFiles: Long,
      createdTime: Long = System.currentTimeMillis()
  ): LastCheckpointFileInfo =
    LastCheckpointFileInfo(
      version = version,
      size = size,
      sizeInBytes = sizeInBytes,
      numFiles = numFiles,
      createdTime = createdTime,
      format = Some(ParquetCheckpointMetadata.FORMAT_JSON),
      parts = None,
      checksum = None
    )

  /**
   * Creates a new Parquet format checkpoint info.
   */
  def createParquetFormat(
      version: Long,
      size: Long,
      sizeInBytes: Long,
      numFiles: Long,
      createdTime: Long = System.currentTimeMillis(),
      parts: Option[Int] = None,
      checksum: Option[String] = None
  ): LastCheckpointFileInfo =
    LastCheckpointFileInfo(
      version = version,
      size = size,
      sizeInBytes = sizeInBytes,
      numFiles = numFiles,
      createdTime = createdTime,
      format = Some(ParquetCheckpointMetadata.FORMAT_PARQUET),
      parts = parts,
      checksum = checksum
    )
}
