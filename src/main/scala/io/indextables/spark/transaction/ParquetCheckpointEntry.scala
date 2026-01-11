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

import io.indextables.spark.util.JsonUtil

/**
 * Flattened representation of all Action types for Parquet checkpoint serialization.
 *
 * This case class uses a discriminator field (actionType) and optional fields for all action
 * properties to support sparse Parquet columns. Complex types (Map, Seq, Set) are serialized
 * as JSON strings with a "Json" suffix in field names.
 *
 * Supports round-trip conversion to/from: AddAction, RemoveAction, MetadataAction, ProtocolAction, SkipAction
 *
 * @param actionType Discriminator field: "add", "remove", "metadata", "protocol", "skip"
 */
case class ParquetCheckpointEntry(
  // Discriminator
  actionType: String,

  // Common fields (used by multiple action types)
  path: Option[String] = None,
  partitionValuesJson: Option[String] = None,  // Map[String, String] as JSON
  size: Option[Long] = None,
  dataChange: Option[Boolean] = None,
  tagsJson: Option[String] = None,             // Map[String, String] as JSON

  // AddAction specific fields
  modificationTime: Option[Long] = None,
  stats: Option[String] = None,
  minValuesJson: Option[String] = None,        // Map[String, String] as JSON
  maxValuesJson: Option[String] = None,        // Map[String, String] as JSON
  numRecords: Option[Long] = None,
  footerStartOffset: Option[Long] = None,
  footerEndOffset: Option[Long] = None,
  hotcacheStartOffset: Option[Long] = None,
  hotcacheLength: Option[Long] = None,
  hasFooterOffsets: Option[Boolean] = None,
  timeRangeStart: Option[String] = None,
  timeRangeEnd: Option[String] = None,
  splitTagsJson: Option[String] = None,        // Set[String] as JSON array
  deleteOpstamp: Option[Long] = None,
  numMergeOps: Option[Int] = None,
  docMappingJson: Option[String] = None,
  uncompressedSizeBytes: Option[Long] = None,

  // RemoveAction specific fields
  deletionTimestamp: Option[Long] = None,
  extendedFileMetadata: Option[Boolean] = None,

  // MetadataAction specific fields
  id: Option[String] = None,
  name: Option[String] = None,
  description: Option[String] = None,
  formatProvider: Option[String] = None,
  formatOptionsJson: Option[String] = None,    // Map[String, String] as JSON
  schemaString: Option[String] = None,
  partitionColumnsJson: Option[String] = None, // Seq[String] as JSON array
  configurationJson: Option[String] = None,    // Map[String, String] as JSON
  createdTime: Option[Long] = None,

  // ProtocolAction specific fields
  minReaderVersion: Option[Int] = None,
  minWriterVersion: Option[Int] = None,
  readerFeaturesJson: Option[String] = None,   // Set[String] as JSON array
  writerFeaturesJson: Option[String] = None,   // Set[String] as JSON array

  // SkipAction specific fields
  skipTimestamp: Option[Long] = None,
  reason: Option[String] = None,
  operation: Option[String] = None,
  retryAfter: Option[Long] = None,
  skipCount: Option[Int] = None
)

object ParquetCheckpointEntry {

  // Action type constants
  val ACTION_TYPE_ADD      = "add"
  val ACTION_TYPE_REMOVE   = "remove"
  val ACTION_TYPE_METADATA = "metadata"
  val ACTION_TYPE_PROTOCOL = "protocol"
  val ACTION_TYPE_SKIP     = "skip"

  /**
   * Convert any Action to a ParquetCheckpointEntry.
   *
   * @param action The action to convert
   * @return ParquetCheckpointEntry representation
   */
  def fromAction(action: Action): ParquetCheckpointEntry = action match {
    case add: AddAction           => fromAddAction(add)
    case remove: RemoveAction     => fromRemoveAction(remove)
    case metadata: MetadataAction => fromMetadataAction(metadata)
    case protocol: ProtocolAction => fromProtocolAction(protocol)
    case skip: SkipAction         => fromSkipAction(skip)
  }

  /**
   * Convert a ParquetCheckpointEntry back to an Action.
   *
   * @param entry The entry to convert
   * @return The original Action
   * @throws IllegalArgumentException if actionType is unknown
   */
  def toAction(entry: ParquetCheckpointEntry): Action = entry.actionType match {
    case ACTION_TYPE_ADD      => toAddAction(entry)
    case ACTION_TYPE_REMOVE   => toRemoveAction(entry)
    case ACTION_TYPE_METADATA => toMetadataAction(entry)
    case ACTION_TYPE_PROTOCOL => toProtocolAction(entry)
    case ACTION_TYPE_SKIP     => toSkipAction(entry)
    case other                => throw new IllegalArgumentException(s"Unknown action type: $other")
  }

  // ============================================================================
  // AddAction conversion
  // ============================================================================

  private def fromAddAction(add: AddAction): ParquetCheckpointEntry = {
    ParquetCheckpointEntry(
      actionType = ACTION_TYPE_ADD,
      path = Some(add.path),
      partitionValuesJson = Some(mapToJson(add.partitionValues)),
      size = Some(add.size),
      modificationTime = Some(add.modificationTime),
      dataChange = Some(add.dataChange),
      stats = add.stats,
      tagsJson = add.tags.map(mapToJson),
      minValuesJson = add.minValues.map(mapToJson),
      maxValuesJson = add.maxValues.map(mapToJson),
      numRecords = add.numRecords,
      footerStartOffset = add.footerStartOffset,
      footerEndOffset = add.footerEndOffset,
      hotcacheStartOffset = add.hotcacheStartOffset,
      hotcacheLength = add.hotcacheLength,
      hasFooterOffsets = Some(add.hasFooterOffsets),
      timeRangeStart = add.timeRangeStart,
      timeRangeEnd = add.timeRangeEnd,
      splitTagsJson = add.splitTags.map(setToJson),
      deleteOpstamp = add.deleteOpstamp,
      numMergeOps = add.numMergeOps,
      docMappingJson = add.docMappingJson,
      uncompressedSizeBytes = add.uncompressedSizeBytes
    )
  }

  private def toAddAction(entry: ParquetCheckpointEntry): AddAction = {
    AddAction(
      path = entry.path.getOrElse(
        throw new IllegalArgumentException("AddAction requires path")
      ),
      partitionValues = entry.partitionValuesJson
        .map(jsonToMap)
        .getOrElse(Map.empty),
      size = entry.size.getOrElse(
        throw new IllegalArgumentException("AddAction requires size")
      ),
      modificationTime = entry.modificationTime.getOrElse(
        throw new IllegalArgumentException("AddAction requires modificationTime")
      ),
      dataChange = entry.dataChange.getOrElse(true),
      stats = entry.stats,
      tags = entry.tagsJson.map(jsonToMap),
      minValues = entry.minValuesJson.map(jsonToMap),
      maxValues = entry.maxValuesJson.map(jsonToMap),
      numRecords = entry.numRecords,
      footerStartOffset = entry.footerStartOffset,
      footerEndOffset = entry.footerEndOffset,
      hotcacheStartOffset = entry.hotcacheStartOffset,
      hotcacheLength = entry.hotcacheLength,
      hasFooterOffsets = entry.hasFooterOffsets.getOrElse(false),
      timeRangeStart = entry.timeRangeStart,
      timeRangeEnd = entry.timeRangeEnd,
      splitTags = entry.splitTagsJson.map(jsonToSet),
      deleteOpstamp = entry.deleteOpstamp,
      numMergeOps = entry.numMergeOps,
      docMappingJson = entry.docMappingJson,
      uncompressedSizeBytes = entry.uncompressedSizeBytes
    )
  }

  // ============================================================================
  // RemoveAction conversion
  // ============================================================================

  private def fromRemoveAction(remove: RemoveAction): ParquetCheckpointEntry = {
    ParquetCheckpointEntry(
      actionType = ACTION_TYPE_REMOVE,
      path = Some(remove.path),
      deletionTimestamp = remove.deletionTimestamp,
      dataChange = Some(remove.dataChange),
      extendedFileMetadata = remove.extendedFileMetadata,
      partitionValuesJson = remove.partitionValues.map(mapToJson),
      size = remove.size,
      tagsJson = remove.tags.map(mapToJson)
    )
  }

  private def toRemoveAction(entry: ParquetCheckpointEntry): RemoveAction = {
    RemoveAction(
      path = entry.path.getOrElse(
        throw new IllegalArgumentException("RemoveAction requires path")
      ),
      deletionTimestamp = entry.deletionTimestamp,
      dataChange = entry.dataChange.getOrElse(true),
      extendedFileMetadata = entry.extendedFileMetadata,
      partitionValues = entry.partitionValuesJson.map(jsonToMap),
      size = entry.size,
      tags = entry.tagsJson.map(jsonToMap)
    )
  }

  // ============================================================================
  // MetadataAction conversion
  // ============================================================================

  private def fromMetadataAction(metadata: MetadataAction): ParquetCheckpointEntry = {
    ParquetCheckpointEntry(
      actionType = ACTION_TYPE_METADATA,
      id = Some(metadata.id),
      name = metadata.name,
      description = metadata.description,
      formatProvider = Some(metadata.format.provider),
      formatOptionsJson = Some(mapToJson(metadata.format.options)),
      schemaString = Some(metadata.schemaString),
      partitionColumnsJson = Some(seqToJson(metadata.partitionColumns)),
      configurationJson = Some(mapToJson(metadata.configuration)),
      createdTime = metadata.createdTime
    )
  }

  private def toMetadataAction(entry: ParquetCheckpointEntry): MetadataAction = {
    MetadataAction(
      id = entry.id.getOrElse(
        throw new IllegalArgumentException("MetadataAction requires id")
      ),
      name = entry.name,
      description = entry.description,
      format = FileFormat(
        provider = entry.formatProvider.getOrElse(
          throw new IllegalArgumentException("MetadataAction requires formatProvider")
        ),
        options = entry.formatOptionsJson
          .map(jsonToMap)
          .getOrElse(Map.empty)
      ),
      schemaString = entry.schemaString.getOrElse(
        throw new IllegalArgumentException("MetadataAction requires schemaString")
      ),
      partitionColumns = entry.partitionColumnsJson
        .map(jsonToSeq)
        .getOrElse(Seq.empty),
      configuration = entry.configurationJson
        .map(jsonToMap)
        .getOrElse(Map.empty),
      createdTime = entry.createdTime
    )
  }

  // ============================================================================
  // ProtocolAction conversion
  // ============================================================================

  private def fromProtocolAction(protocol: ProtocolAction): ParquetCheckpointEntry = {
    ParquetCheckpointEntry(
      actionType = ACTION_TYPE_PROTOCOL,
      minReaderVersion = Some(protocol.minReaderVersion),
      minWriterVersion = Some(protocol.minWriterVersion),
      readerFeaturesJson = protocol.readerFeatures.map(setToJson),
      writerFeaturesJson = protocol.writerFeatures.map(setToJson)
    )
  }

  private def toProtocolAction(entry: ParquetCheckpointEntry): ProtocolAction = {
    ProtocolAction(
      minReaderVersion = entry.minReaderVersion.getOrElse(
        throw new IllegalArgumentException("ProtocolAction requires minReaderVersion")
      ),
      minWriterVersion = entry.minWriterVersion.getOrElse(
        throw new IllegalArgumentException("ProtocolAction requires minWriterVersion")
      ),
      readerFeatures = entry.readerFeaturesJson.map(jsonToSet),
      writerFeatures = entry.writerFeaturesJson.map(jsonToSet)
    )
  }

  // ============================================================================
  // SkipAction conversion
  // ============================================================================

  private def fromSkipAction(skip: SkipAction): ParquetCheckpointEntry = {
    ParquetCheckpointEntry(
      actionType = ACTION_TYPE_SKIP,
      path = Some(skip.path),
      skipTimestamp = Some(skip.skipTimestamp),
      reason = Some(skip.reason),
      operation = Some(skip.operation),
      partitionValuesJson = skip.partitionValues.map(mapToJson),
      size = skip.size,
      retryAfter = skip.retryAfter,
      skipCount = Some(skip.skipCount)
    )
  }

  private def toSkipAction(entry: ParquetCheckpointEntry): SkipAction = {
    SkipAction(
      path = entry.path.getOrElse(
        throw new IllegalArgumentException("SkipAction requires path")
      ),
      skipTimestamp = entry.skipTimestamp.getOrElse(
        throw new IllegalArgumentException("SkipAction requires skipTimestamp")
      ),
      reason = entry.reason.getOrElse(
        throw new IllegalArgumentException("SkipAction requires reason")
      ),
      operation = entry.operation.getOrElse(
        throw new IllegalArgumentException("SkipAction requires operation")
      ),
      partitionValues = entry.partitionValuesJson.map(jsonToMap),
      size = entry.size,
      retryAfter = entry.retryAfter,
      skipCount = entry.skipCount.getOrElse(1)
    )
  }

  // ============================================================================
  // JSON serialization helpers
  // ============================================================================

  private def mapToJson(map: Map[String, String]): String = {
    JsonUtil.toJson(map)
  }

  private def jsonToMap(json: String): Map[String, String] = {
    if (json == null || json.isEmpty || json == "{}") {
      Map.empty
    } else {
      JsonUtil.mapper.readValue(json, classOf[Map[String, String]])
    }
  }

  private def seqToJson(seq: Seq[String]): String = {
    JsonUtil.toJson(seq)
  }

  private def jsonToSeq(json: String): Seq[String] = {
    if (json == null || json.isEmpty || json == "[]") {
      Seq.empty
    } else {
      JsonUtil.parseStringArray(json)
    }
  }

  private def setToJson(set: Set[String]): String = {
    JsonUtil.toJson(set.toSeq)
  }

  private def jsonToSet(json: String): Set[String] = {
    if (json == null || json.isEmpty || json == "[]") {
      Set.empty
    } else {
      JsonUtil.parseStringArray(json).toSet
    }
  }
}
