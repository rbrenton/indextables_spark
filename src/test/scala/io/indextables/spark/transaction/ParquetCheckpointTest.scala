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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.TestBase

/**
 * Comprehensive test suite for Parquet checkpoint functionality.
 *
 * Tests cover:
 *   - ParquetCheckpointEntry round-trip conversions for all action types
 *   - ParquetCheckpointWriter checkpoint creation and validation
 *   - ParquetCheckpointReader checkpoint reading with optimizations
 *   - Integration tests for write/read round-trips
 *   - Error handling for edge cases
 */
class ParquetCheckpointTest extends TestBase {

  // ==========================================================================
  // ParquetCheckpointEntry Round-Trip Tests
  // ==========================================================================

  test("AddAction round-trip preserves all fields") {
    val original = AddAction(
      path = "splits/test-split-12345.split",
      partitionValues = Map("date" -> "2024-01-15", "region" -> "us-east"),
      size = 1048576L,
      modificationTime = 1705312000000L,
      dataChange = true,
      stats = Some("""{"numRecords":1000,"minValues":{"id":"1"},"maxValues":{"id":"1000"}}"""),
      tags = Some(Map("source" -> "batch", "version" -> "1.0")),
      minValues = Some(Map("id" -> "1", "timestamp" -> "2024-01-01T00:00:00Z")),
      maxValues = Some(Map("id" -> "1000", "timestamp" -> "2024-01-31T23:59:59Z")),
      numRecords = Some(1000L),
      footerStartOffset = Some(1000000L),
      footerEndOffset = Some(1048576L),
      hotcacheStartOffset = Some(900000L),
      hotcacheLength = Some(148576L),
      hasFooterOffsets = true,
      timeRangeStart = Some("2024-01-01T00:00:00Z"),
      timeRangeEnd = Some("2024-01-31T23:59:59Z"),
      splitTags = Some(Set("indexed", "searchable", "compressed")),
      deleteOpstamp = Some(42L),
      numMergeOps = Some(3),
      docMappingJson = Some("""{"fields":["id","name","content"]}"""),
      uncompressedSizeBytes = Some(2097152L)
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[AddAction]

    // Verify all fields are preserved
    assert(restored.path === original.path, "path mismatch")
    assert(restored.partitionValues === original.partitionValues, "partitionValues mismatch")
    assert(restored.size === original.size, "size mismatch")
    assert(restored.modificationTime === original.modificationTime, "modificationTime mismatch")
    assert(restored.dataChange === original.dataChange, "dataChange mismatch")
    assert(restored.stats === original.stats, "stats mismatch")
    assert(restored.tags === original.tags, "tags mismatch")
    assert(restored.minValues === original.minValues, "minValues mismatch")
    assert(restored.maxValues === original.maxValues, "maxValues mismatch")
    assert(restored.numRecords === original.numRecords, "numRecords mismatch")
    assert(restored.footerStartOffset === original.footerStartOffset, "footerStartOffset mismatch")
    assert(restored.footerEndOffset === original.footerEndOffset, "footerEndOffset mismatch")
    assert(restored.hotcacheStartOffset === original.hotcacheStartOffset, "hotcacheStartOffset mismatch")
    assert(restored.hotcacheLength === original.hotcacheLength, "hotcacheLength mismatch")
    assert(restored.hasFooterOffsets === original.hasFooterOffsets, "hasFooterOffsets mismatch")
    assert(restored.timeRangeStart === original.timeRangeStart, "timeRangeStart mismatch")
    assert(restored.timeRangeEnd === original.timeRangeEnd, "timeRangeEnd mismatch")
    assert(restored.splitTags === original.splitTags, "splitTags mismatch")
    assert(restored.deleteOpstamp === original.deleteOpstamp, "deleteOpstamp mismatch")
    assert(restored.numMergeOps === original.numMergeOps, "numMergeOps mismatch")
    assert(restored.docMappingJson === original.docMappingJson, "docMappingJson mismatch")
    assert(restored.uncompressedSizeBytes === original.uncompressedSizeBytes, "uncompressedSizeBytes mismatch")
  }

  test("AddAction round-trip with minimal fields (only required)") {
    val original = AddAction(
      path = "minimal-split.split",
      partitionValues = Map.empty,
      size = 100L,
      modificationTime = System.currentTimeMillis(),
      dataChange = false
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[AddAction]

    assert(restored.path === original.path)
    assert(restored.partitionValues === original.partitionValues)
    assert(restored.size === original.size)
    assert(restored.modificationTime === original.modificationTime)
    assert(restored.dataChange === original.dataChange)

    // Optional fields should be None or default
    assert(restored.stats === None)
    assert(restored.tags === None)
    assert(restored.minValues === None)
    assert(restored.maxValues === None)
    assert(restored.numRecords === None)
    assert(restored.footerStartOffset === None)
    assert(restored.footerEndOffset === None)
    assert(restored.hasFooterOffsets === false)
    assert(restored.splitTags === None)
  }

  test("RemoveAction round-trip preserves all fields") {
    val original = RemoveAction(
      path = "removed-split.split",
      deletionTimestamp = Some(1705312000000L),
      dataChange = true,
      extendedFileMetadata = Some(true),
      partitionValues = Some(Map("year" -> "2024", "month" -> "01")),
      size = Some(512000L),
      tags = Some(Map("reason" -> "compaction", "requestedBy" -> "admin"))
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[RemoveAction]

    assert(restored.path === original.path, "path mismatch")
    assert(restored.deletionTimestamp === original.deletionTimestamp, "deletionTimestamp mismatch")
    assert(restored.dataChange === original.dataChange, "dataChange mismatch")
    assert(restored.extendedFileMetadata === original.extendedFileMetadata, "extendedFileMetadata mismatch")
    assert(restored.partitionValues === original.partitionValues, "partitionValues mismatch")
    assert(restored.size === original.size, "size mismatch")
    assert(restored.tags === original.tags, "tags mismatch")
  }

  test("RemoveAction round-trip with minimal fields") {
    val original = RemoveAction(
      path = "minimal-remove.split",
      deletionTimestamp = None,
      dataChange = false,
      extendedFileMetadata = None,
      partitionValues = None,
      size = None,
      tags = None
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[RemoveAction]

    assert(restored.path === original.path)
    assert(restored.deletionTimestamp === None)
    assert(restored.dataChange === original.dataChange)
    assert(restored.extendedFileMetadata === None)
    assert(restored.partitionValues === None)
    assert(restored.size === None)
    assert(restored.tags === None)
  }

  test("MetadataAction round-trip preserves all fields") {
    val original = MetadataAction(
      id = "table-12345-uuid",
      name = Some("my_indexed_table"),
      description = Some("Test table for parquet checkpoint tests"),
      format = FileFormat(
        provider = "io.indextables.spark.core.IndexTables4SparkTableProvider",
        options = Map("compression" -> "snappy", "indexing.mode" -> "full")
      ),
      schemaString = """{"type":"struct","fields":[{"name":"id","type":"integer"},{"name":"content","type":"string"}]}""",
      partitionColumns = Seq("date", "region"),
      configuration = Map(
        "spark.indextables.indexing.typemap.content" -> "text",
        "spark.indextables.indexing.fastfields" -> "score"
      ),
      createdTime = Some(1705312000000L)
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[MetadataAction]

    assert(restored.id === original.id, "id mismatch")
    assert(restored.name === original.name, "name mismatch")
    assert(restored.description === original.description, "description mismatch")
    assert(restored.format.provider === original.format.provider, "format.provider mismatch")
    assert(restored.format.options === original.format.options, "format.options mismatch")
    assert(restored.schemaString === original.schemaString, "schemaString mismatch")
    assert(restored.partitionColumns === original.partitionColumns, "partitionColumns mismatch")
    assert(restored.configuration === original.configuration, "configuration mismatch")
    assert(restored.createdTime === original.createdTime, "createdTime mismatch")
  }

  test("MetadataAction round-trip with minimal fields") {
    val original = MetadataAction(
      id = "minimal-table-id",
      name = None,
      description = None,
      format = FileFormat(provider = "indextables", options = Map.empty),
      schemaString = """{"type":"struct","fields":[]}""",
      partitionColumns = Seq.empty,
      configuration = Map.empty,
      createdTime = None
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[MetadataAction]

    assert(restored.id === original.id)
    assert(restored.name === None)
    assert(restored.description === None)
    assert(restored.format.provider === original.format.provider)
    assert(restored.format.options === Map.empty)
    assert(restored.schemaString === original.schemaString)
    assert(restored.partitionColumns === Seq.empty)
    assert(restored.configuration === Map.empty)
    assert(restored.createdTime === None)
  }

  test("ProtocolAction round-trip preserves all fields") {
    val original = ProtocolAction(
      minReaderVersion = 2,
      minWriterVersion = 3,
      readerFeatures = Some(Set("columnMapping", "deletionVectors")),
      writerFeatures = Some(Set("appendOnly", "invariants", "checkConstraints"))
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[ProtocolAction]

    assert(restored.minReaderVersion === original.minReaderVersion, "minReaderVersion mismatch")
    assert(restored.minWriterVersion === original.minWriterVersion, "minWriterVersion mismatch")
    assert(restored.readerFeatures === original.readerFeatures, "readerFeatures mismatch")
    assert(restored.writerFeatures === original.writerFeatures, "writerFeatures mismatch")
  }

  test("ProtocolAction round-trip with minimal fields") {
    val original = ProtocolAction(
      minReaderVersion = 1,
      minWriterVersion = 1,
      readerFeatures = None,
      writerFeatures = None
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[ProtocolAction]

    assert(restored.minReaderVersion === 1)
    assert(restored.minWriterVersion === 1)
    assert(restored.readerFeatures === None)
    assert(restored.writerFeatures === None)
  }

  test("SkipAction round-trip preserves all fields") {
    val original = SkipAction(
      path = "skipped-split.split",
      skipTimestamp = 1705312000000L,
      reason = "File corrupted during network transfer",
      operation = "merge",
      partitionValues = Some(Map("date" -> "2024-01-15")),
      size = Some(1024000L),
      retryAfter = Some(1705315600000L),
      skipCount = 3
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[SkipAction]

    assert(restored.path === original.path, "path mismatch")
    assert(restored.skipTimestamp === original.skipTimestamp, "skipTimestamp mismatch")
    assert(restored.reason === original.reason, "reason mismatch")
    assert(restored.operation === original.operation, "operation mismatch")
    assert(restored.partitionValues === original.partitionValues, "partitionValues mismatch")
    assert(restored.size === original.size, "size mismatch")
    assert(restored.retryAfter === original.retryAfter, "retryAfter mismatch")
    assert(restored.skipCount === original.skipCount, "skipCount mismatch")
  }

  test("SkipAction round-trip with minimal fields") {
    val original = SkipAction(
      path = "minimal-skip.split",
      skipTimestamp = System.currentTimeMillis(),
      reason = "Temporary skip",
      operation = "read"
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[SkipAction]

    assert(restored.path === original.path)
    assert(restored.skipTimestamp === original.skipTimestamp)
    assert(restored.reason === original.reason)
    assert(restored.operation === original.operation)
    assert(restored.partitionValues === None)
    assert(restored.size === None)
    assert(restored.retryAfter === None)
    assert(restored.skipCount === 1) // Default value
  }

  test("ParquetCheckpointEntry.toAction throws on unknown action type") {
    val invalidEntry = ParquetCheckpointEntry(
      actionType = "unknown_type",
      path = Some("test.split")
    )

    val thrown = intercept[IllegalArgumentException] {
      ParquetCheckpointEntry.toAction(invalidEntry)
    }

    assert(thrown.getMessage.contains("Unknown action type"))
    assert(thrown.getMessage.contains("unknown_type"))
  }

  test("ParquetCheckpointEntry.toAction throws when required fields are missing for AddAction") {
    val invalidEntry = ParquetCheckpointEntry(
      actionType = ParquetCheckpointEntry.ACTION_TYPE_ADD,
      path = None, // Missing required path
      size = Some(100L),
      modificationTime = Some(System.currentTimeMillis())
    )

    val thrown = intercept[IllegalArgumentException] {
      ParquetCheckpointEntry.toAction(invalidEntry)
    }

    assert(thrown.getMessage.contains("AddAction requires path"))
  }

  // ==========================================================================
  // ParquetCheckpointWriter Tests
  // ==========================================================================

  test("ParquetCheckpointWriter should write checkpoint with mixed action types") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        // Ensure transaction log directory exists
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val actions: Seq[Action] = Seq(
          ProtocolAction(minReaderVersion = 1, minWriterVersion = 2),
          MetadataAction(
            id = "test-table",
            name = Some("Test Table"),
            description = None,
            format = FileFormat("indextables", Map.empty),
            schemaString = """{"type":"struct","fields":[]}""",
            partitionColumns = Seq.empty,
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          ),
          AddAction(
            path = "file1.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          ),
          AddAction(
            path = "file2.split",
            partitionValues = Map("date" -> "2024-01-15"),
            size = 2000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(200L)
          ),
          RemoveAction(
            path = "old-file.split",
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true,
            extendedFileMetadata = None,
            partitionValues = None,
            size = Some(500L)
          )
        )

        val metadata = writer.writeCheckpoint(version = 10L, actions = actions)

        // Verify metadata
        assert(metadata.version === 10L)
        assert(metadata.size === 5L) // Total actions
        assert(metadata.numFiles === 2L) // Only AddActions count
        assert(metadata.format === ParquetCheckpointMetadata.FORMAT_PARQUET)
        assert(metadata.sizeInBytes > 0L)
        assert(metadata.createdTime > 0L)

        // Verify checkpoint file exists
        val checkpointPath = writer.getCheckpointPath(10L)
        assert(cloudProvider.exists(checkpointPath.toString), "Checkpoint file should exist")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointWriter should write checkpoint with only AddActions") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val addActions = (1 to 10).map { i =>
          AddAction(
            path = s"split-$i.split",
            partitionValues = Map("partition" -> (i % 3).toString),
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 100L)
          )
        }

        val metadata = writer.writeCheckpoint(version = 5L, actions = addActions)

        assert(metadata.version === 5L)
        assert(metadata.size === 10L)
        assert(metadata.numFiles === 10L)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointWriter should use configured compression") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(
        Map("spark.indextables.checkpoint.parquet.compression" -> "gzip").asJava
      )
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val actions = Seq(
          AddAction(
            path = "test.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )
        )

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        assert(metadata.version === 1L)
        assert(metadata.sizeInBytes > 0L)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointWriter should validate checkpoint when validation enabled") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(
        Map("spark.indextables.checkpoint.validateOnWrite" -> "true").asJava
      )
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val actions = Seq(
          ProtocolAction(1, 1),
          AddAction("file.split", Map.empty, 100L, System.currentTimeMillis(), true)
        )

        // Should not throw if validation passes
        val metadata = writer.writeCheckpoint(version = 3L, actions = actions)
        assert(metadata.size === 2L)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointWriter should update _last_checkpoint file") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val actions = Seq(
          AddAction("file.split", Map.empty, 100L, System.currentTimeMillis(), true)
        )

        writer.writeCheckpoint(version = 7L, actions = actions)

        // Verify _last_checkpoint file exists
        val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")
        assert(cloudProvider.exists(lastCheckpointPath.toString), "_last_checkpoint file should exist")

        // Read and verify content
        val content = new String(cloudProvider.readFile(lastCheckpointPath.toString), "UTF-8")
        assert(content.contains("\"version\":7"))
        assert(content.contains("\"format\":\"parquet\""))
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // ParquetCheckpointReader Tests
  // ==========================================================================

  test("ParquetCheckpointReader should read checkpoint with all action types") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        val originalActions: Seq[Action] = Seq(
          ProtocolAction(1, 2),
          MetadataAction(
            "table-id",
            Some("Table"),
            None,
            FileFormat("provider", Map.empty),
            "schema",
            Seq("col"),
            Map.empty,
            Some(1000L)
          ),
          AddAction("add1.split", Map("p" -> "v"), 100L, 2000L, true, numRecords = Some(10L)),
          AddAction("add2.split", Map.empty, 200L, 3000L, false),
          RemoveAction("rem.split", Some(4000L), true, None, None, Some(50L))
        )

        writer.writeCheckpoint(version = 15L, actions = originalActions)

        val readActionsOpt = reader.readParquetCheckpoint(15L)

        assert(readActionsOpt.isDefined, "Should successfully read checkpoint")
        val readActions = readActionsOpt.get

        assert(readActions.length === originalActions.length)

        // Verify each action type is present
        assert(readActions.count(_.isInstanceOf[ProtocolAction]) === 1)
        assert(readActions.count(_.isInstanceOf[MetadataAction]) === 1)
        assert(readActions.count(_.isInstanceOf[AddAction]) === 2)
        assert(readActions.count(_.isInstanceOf[RemoveAction]) === 1)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader.getAddActionsFromCheckpoint optimization") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create checkpoint with many different action types
        val actions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction("id", None, None, FileFormat("p", Map.empty), "s", Seq.empty, Map.empty, None)
        ) ++ (1 to 100).map { i =>
          AddAction(s"file$i.split", Map("idx" -> i.toString), i * 100L, System.currentTimeMillis(), true, numRecords = Some(i * 10L))
        } ++ Seq(
          RemoveAction("removed.split", Some(System.currentTimeMillis()), true, None, None, None)
        )

        writer.writeCheckpoint(version = 20L, actions = actions)

        // Test optimized AddAction-only read
        val addActionsOpt = reader.getAddActionsFromCheckpoint()

        assert(addActionsOpt.isDefined)
        val addActions = addActionsOpt.get

        assert(addActions.length === 100, "Should only return AddActions")
        addActions.foreach { action =>
          assert(action.path.startsWith("file"), "All actions should be AddActions with correct paths")
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader should return None for non-existent checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        val result = reader.readParquetCheckpoint(999L)

        assert(result.isEmpty, "Should return None for non-existent checkpoint")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader.getLastCheckpointInfo should read LastCheckpointFileInfo") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        val actions = Seq(
          AddAction("test.split", Map.empty, 1000L, System.currentTimeMillis(), true)
        )

        writer.writeCheckpoint(version = 25L, actions = actions)

        val infoOpt = reader.getLastCheckpointInfo()

        assert(infoOpt.isDefined)
        val info = infoOpt.get

        assert(info.version === 25L)
        assert(info.size === 1L)
        assert(info.numFiles === 1L)
        assert(info.isParquetFormat)
        assert(!info.isJsonFormat)
        assert(!info.isMultiPart)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader.readActionsOfTypes should filter by action types") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        val actions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction("id", None, None, FileFormat("p", Map.empty), "s", Seq.empty, Map.empty, None),
          AddAction("add1.split", Map.empty, 100L, System.currentTimeMillis(), true),
          AddAction("add2.split", Map.empty, 200L, System.currentTimeMillis(), true),
          RemoveAction("rem1.split", None, true, None, None, None),
          RemoveAction("rem2.split", None, true, None, None, None),
          RemoveAction("rem3.split", None, true, None, None, None)
        )

        writer.writeCheckpoint(version = 30L, actions = actions)

        // Filter for add and remove only
        val filteredOpt = reader.readActionsOfTypes(30L, Set("add", "remove"))

        assert(filteredOpt.isDefined)
        val filtered = filteredOpt.get

        assert(filtered.length === 5) // 2 adds + 3 removes
        assert(filtered.count(_.isInstanceOf[AddAction]) === 2)
        assert(filtered.count(_.isInstanceOf[RemoveAction]) === 3)
        assert(filtered.count(_.isInstanceOf[ProtocolAction]) === 0)
        assert(filtered.count(_.isInstanceOf[MetadataAction]) === 0)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Integration Tests
  // ==========================================================================

  test("Integration: write then read round-trip preserves all data") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create comprehensive test data
        val protocolAction = ProtocolAction(
          minReaderVersion = 2,
          minWriterVersion = 3,
          readerFeatures = Some(Set("feature1", "feature2")),
          writerFeatures = Some(Set("feature3"))
        )

        val metadataAction = MetadataAction(
          id = "comprehensive-test-table",
          name = Some("Comprehensive Test Table"),
          description = Some("A table for comprehensive round-trip testing"),
          format = FileFormat(
            provider = "io.indextables.spark.core.IndexTables4SparkTableProvider",
            options = Map("key1" -> "value1", "key2" -> "value2")
          ),
          schemaString = """{"type":"struct","fields":[{"name":"id","type":"integer"}]}""",
          partitionColumns = Seq("date", "region"),
          configuration = Map("config1" -> "configValue1"),
          createdTime = Some(1705312000000L)
        )

        val addActions = (1 to 5).map { i =>
          AddAction(
            path = s"comprehensive-file-$i.split",
            partitionValues = Map("date" -> s"2024-01-${10 + i}", "region" -> s"region-$i"),
            size = i * 10000L,
            modificationTime = 1705312000000L + i * 1000,
            dataChange = i % 2 == 0,
            stats = Some(s"""{"numRecords":${i * 100}}"""),
            tags = Some(Map("tag" -> s"value$i")),
            minValues = Some(Map("id" -> s"${i * 100}")),
            maxValues = Some(Map("id" -> s"${i * 100 + 99}")),
            numRecords = Some(i * 100L),
            footerStartOffset = Some(i * 1000L),
            footerEndOffset = Some(i * 10000L),
            hasFooterOffsets = true
          )
        }

        val removeActions = Seq(
          RemoveAction(
            path = "removed-file-1.split",
            deletionTimestamp = Some(1705312000000L),
            dataChange = true,
            extendedFileMetadata = Some(true),
            partitionValues = Some(Map("date" -> "2024-01-01")),
            size = Some(5000L),
            tags = Some(Map("reason" -> "compaction"))
          )
        )

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions ++ removeActions

        // Write checkpoint
        val writeMetadata = writer.writeCheckpoint(version = 50L, actions = allActions)

        assert(writeMetadata.version === 50L)
        assert(writeMetadata.size === allActions.length)
        assert(writeMetadata.numFiles === 5L) // 5 AddActions

        // Read checkpoint back
        val readActionsOpt = reader.getActionsFromCheckpoint()

        assert(readActionsOpt.isDefined)
        val readActions = readActionsOpt.get

        assert(readActions.length === allActions.length, "Action count should match")

        // Verify protocol action
        val readProtocol = readActions.collectFirst { case p: ProtocolAction => p }.get
        assert(readProtocol.minReaderVersion === protocolAction.minReaderVersion)
        assert(readProtocol.minWriterVersion === protocolAction.minWriterVersion)
        assert(readProtocol.readerFeatures === protocolAction.readerFeatures)
        assert(readProtocol.writerFeatures === protocolAction.writerFeatures)

        // Verify metadata action
        val readMetadata = readActions.collectFirst { case m: MetadataAction => m }.get
        assert(readMetadata.id === metadataAction.id)
        assert(readMetadata.name === metadataAction.name)
        assert(readMetadata.description === metadataAction.description)
        assert(readMetadata.format.provider === metadataAction.format.provider)
        assert(readMetadata.format.options === metadataAction.format.options)
        assert(readMetadata.partitionColumns === metadataAction.partitionColumns)

        // Verify add actions
        val readAddActions = readActions.collect { case a: AddAction => a }
        assert(readAddActions.length === 5)
        readAddActions.zip(addActions).foreach { case (read, original) =>
          assert(read.path === original.path)
          assert(read.partitionValues === original.partitionValues)
          assert(read.size === original.size)
          assert(read.numRecords === original.numRecords)
          assert(read.hasFooterOffsets === original.hasFooterOffsets)
        }

        // Verify remove action
        val readRemove = readActions.collectFirst { case r: RemoveAction => r }.get
        assert(readRemove.path === removeActions.head.path)
        assert(readRemove.deletionTimestamp === removeActions.head.deletionTimestamp)
        assert(readRemove.tags === removeActions.head.tags)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("Integration: checkpointExistsForVersion should correctly detect checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Initially no checkpoint exists
        assert(!reader.checkpointExistsForVersion(100L))

        // Create checkpoint
        val actions = Seq(
          AddAction("test.split", Map.empty, 100L, System.currentTimeMillis(), true)
        )
        writer.writeCheckpoint(version = 100L, actions = actions)

        // Now checkpoint exists
        assert(reader.checkpointExistsForVersion(100L))

        // Other versions still don't exist
        assert(!reader.checkpointExistsForVersion(99L))
        assert(!reader.checkpointExistsForVersion(101L))
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("Integration: multiple checkpoints at different versions") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create first checkpoint
        val actions1 = Seq(
          ProtocolAction(1, 1),
          AddAction("v10-file.split", Map.empty, 100L, System.currentTimeMillis(), true)
        )
        writer.writeCheckpoint(version = 10L, actions = actions1)

        // Create second checkpoint
        val actions2 = Seq(
          ProtocolAction(1, 1),
          AddAction("v10-file.split", Map.empty, 100L, System.currentTimeMillis(), true),
          AddAction("v20-file.split", Map.empty, 200L, System.currentTimeMillis(), true)
        )
        writer.writeCheckpoint(version = 20L, actions = actions2)

        // Verify both checkpoints exist
        assert(reader.checkpointExistsForVersion(10L))
        assert(reader.checkpointExistsForVersion(20L))

        // Verify last checkpoint points to version 20
        val lastInfo = reader.getLastCheckpointInfo()
        assert(lastInfo.isDefined)
        assert(lastInfo.get.version === 20L)
        assert(lastInfo.get.numFiles === 2L)

        // Can still read the older checkpoint directly
        val v10Actions = reader.readParquetCheckpoint(10L)
        assert(v10Actions.isDefined)
        assert(v10Actions.get.length === 2)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Metadata and Compatibility Tests
  // ==========================================================================

  test("ParquetCheckpointMetadata.create should correctly count files") {
    val actions: Seq[Action] = Seq(
      ProtocolAction(1, 1),
      MetadataAction("id", None, None, FileFormat("p", Map.empty), "s", Seq.empty, Map.empty, None),
      AddAction("f1.split", Map.empty, 100L, 1000L, true),
      AddAction("f2.split", Map.empty, 200L, 2000L, true),
      AddAction("f3.split", Map.empty, 300L, 3000L, true),
      RemoveAction("r1.split", None, true, None, None, None),
      RemoveAction("r2.split", None, true, None, None, None)
    )

    val metadata = ParquetCheckpointMetadata.create(
      version = 100L,
      actions = actions,
      sizeInBytes = 5000L
    )

    assert(metadata.version === 100L)
    assert(metadata.size === 7L) // Total actions
    assert(metadata.numFiles === 3L) // Only AddActions
    assert(metadata.sizeInBytes === 5000L)
    assert(metadata.format === ParquetCheckpointMetadata.FORMAT_PARQUET)
    assert(!metadata.isMultiPart)
  }

  test("LastCheckpointFileInfo.isParquetFormat and isJsonFormat") {
    val parquetInfo = LastCheckpointFileInfo(
      version = 10L,
      size = 5L,
      sizeInBytes = 1000L,
      numFiles = 3L,
      createdTime = System.currentTimeMillis(),
      format = Some(ParquetCheckpointMetadata.FORMAT_PARQUET)
    )

    assert(parquetInfo.isParquetFormat)
    assert(!parquetInfo.isJsonFormat)

    val jsonInfo = LastCheckpointFileInfo(
      version = 10L,
      size = 5L,
      sizeInBytes = 1000L,
      numFiles = 3L,
      createdTime = System.currentTimeMillis(),
      format = Some(ParquetCheckpointMetadata.FORMAT_JSON)
    )

    assert(!jsonInfo.isParquetFormat)
    assert(jsonInfo.isJsonFormat)

    val legacyInfo = LastCheckpointFileInfo(
      version = 10L,
      size = 5L,
      sizeInBytes = 1000L,
      numFiles = 3L,
      createdTime = System.currentTimeMillis(),
      format = None
    )

    assert(!legacyInfo.isParquetFormat)
    assert(legacyInfo.isJsonFormat) // Legacy format is JSON
  }

  test("LastCheckpointFileInfo.toCheckpointInfo conversion") {
    val original = LastCheckpointFileInfo(
      version = 25L,
      size = 100L,
      sizeInBytes = 50000L,
      numFiles = 80L,
      createdTime = 1705312000000L,
      format = Some("parquet"),
      parts = Some(3),
      checksum = Some("abc123")
    )

    val converted = original.toCheckpointInfo

    assert(converted.version === 25L)
    assert(converted.size === 100L)
    assert(converted.sizeInBytes === 50000L)
    assert(converted.numFiles === 80L)
    assert(converted.createdTime === 1705312000000L)
  }

  test("ParquetCheckpointMetadata.toLastCheckpointFileInfo conversion") {
    val metadata = ParquetCheckpointMetadata(
      version = 30L,
      size = 50L,
      sizeInBytes = 25000L,
      numFiles = 40L,
      createdTime = 1705312000000L,
      format = ParquetCheckpointMetadata.FORMAT_PARQUET,
      parts = Some(2),
      checksum = Some("checksum123")
    )

    val converted = metadata.toLastCheckpointFileInfo

    assert(converted.version === 30L)
    assert(converted.size === 50L)
    assert(converted.sizeInBytes === 25000L)
    assert(converted.numFiles === 40L)
    assert(converted.createdTime === 1705312000000L)
    assert(converted.format === Some("parquet"))
    assert(converted.parts === Some(2))
    assert(converted.checksum === Some("checksum123"))
    assert(converted.isParquetFormat)
    assert(converted.isMultiPart)
  }

  // ==========================================================================
  // Error Handling Tests
  // ==========================================================================

  test("ParquetCheckpointReader should handle missing _last_checkpoint gracefully") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // No _last_checkpoint file exists
        val infoOpt = reader.getLastCheckpointInfo()
        assert(infoOpt.isEmpty)

        // getActionsFromCheckpoint should also return None
        val actionsOpt = reader.getActionsFromCheckpoint()
        assert(actionsOpt.isEmpty)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader should return None when checkpoint file is missing but _last_checkpoint exists") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Manually write _last_checkpoint pointing to non-existent checkpoint
        val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")
        val lastCheckpointContent = """{"version":999,"size":10,"sizeInBytes":1000,"numFiles":8,"createdTime":1705312000000,"format":"parquet"}"""
        cloudProvider.writeFile(lastCheckpointPath.toString, lastCheckpointContent.getBytes("UTF-8"))

        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Last checkpoint info can be read
        val infoOpt = reader.getLastCheckpointInfo()
        assert(infoOpt.isDefined)
        assert(infoOpt.get.version === 999L)

        // But reading the actual checkpoint should return None (file doesn't exist)
        val actionsOpt = reader.readParquetCheckpoint(999L)
        assert(actionsOpt.isEmpty)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader getLastCheckpointInfo returns None for JSON format checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Write a JSON format checkpoint info
        val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")
        val jsonCheckpointInfo = """{"version":50,"size":5,"sizeInBytes":500,"numFiles":3,"createdTime":1705312000000,"format":"json"}"""
        cloudProvider.writeFile(lastCheckpointPath.toString, jsonCheckpointInfo.getBytes("UTF-8"))

        // getLastCheckpointInfo should return the info
        val infoOpt = reader.getLastCheckpointInfo()
        assert(infoOpt.isDefined)
        assert(infoOpt.get.version === 50L)
        assert(infoOpt.get.isJsonFormat)
        assert(!infoOpt.get.isParquetFormat)

        // But getActionsFromCheckpoint should return None because it's not Parquet format
        val actionsOpt = reader.getActionsFromCheckpoint()
        assert(actionsOpt.isEmpty, "Should return None for JSON format checkpoint")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("Empty partition values and tags serialize correctly") {
    val original = AddAction(
      path = "empty-maps.split",
      partitionValues = Map.empty[String, String],
      size = 100L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      tags = Some(Map.empty[String, String]),
      minValues = Some(Map.empty[String, String]),
      maxValues = Some(Map.empty[String, String])
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[AddAction]

    assert(restored.partitionValues === Map.empty)
    assert(restored.tags === Some(Map.empty))
    assert(restored.minValues === Some(Map.empty))
    assert(restored.maxValues === Some(Map.empty))
  }

  test("Special characters in strings are handled correctly") {
    val original = AddAction(
      path = "path/with/special \"chars\"/and\nnewlines.split",
      partitionValues = Map("key with spaces" -> "value\twith\ttabs"),
      size = 100L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = Some("""{"field":"value with \"quotes\""}"""),
      tags = Some(Map("unicode" -> "Hello \u4e16\u754c"))
    )

    val entry = ParquetCheckpointEntry.fromAction(original)
    val restored = ParquetCheckpointEntry.toAction(entry).asInstanceOf[AddAction]

    assert(restored.path === original.path)
    assert(restored.partitionValues === original.partitionValues)
    assert(restored.stats === original.stats)
    assert(restored.tags === original.tags)
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Basic Writing
  // ==========================================================================

  test("multi-part checkpoint should create correct number of parts for 250 actions with max 100 per part") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "100"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Create 250 actions (should split into 3 parts: 100 + 100 + 50 via round-robin)
        val actions: Seq[Action] = (1 to 250).map { i =>
          AddAction(
            path = s"file$i.split",
            partitionValues = Map("idx" -> i.toString),
            size = i * 100L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 10L)
          )
        }

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        // Verify metadata
        assert(metadata.version === 1L)
        assert(metadata.size === 250L)
        assert(metadata.numFiles === 250L)
        assert(metadata.parts.contains(3), "Should have 3 parts for 250 actions with max 100 per part")
        assert(metadata.isMultiPart)

        // Verify all part files exist
        (0 until 3).foreach { part =>
          val partPath = writer.getMultiPartCheckpointPath(1L, part)
          assert(cloudProvider.exists(partPath.toString), s"Part $part file should exist at $partPath")
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("multi-part checkpoint should create parts with correct file naming format") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val actions: Seq[Action] = (1 to 100).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        writer.writeCheckpoint(version = 42L, actions = actions)

        // Verify file naming: _checkpoints/{version:020d}.checkpoint.{part:010d}.parquet
        val checkpointsDir = new Path(transactionLogPath, "_checkpoints")
        val expectedPart0 = new Path(checkpointsDir, "00000000000000000042.checkpoint.0000000000.parquet")
        val expectedPart1 = new Path(checkpointsDir, "00000000000000000042.checkpoint.0000000001.parquet")

        assert(cloudProvider.exists(expectedPart0.toString), s"Part 0 should exist at $expectedPart0")
        assert(cloudProvider.exists(expectedPart1.toString), s"Part 1 should exist at $expectedPart1")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("multi-part checkpoint metadata should have correct parts count in _last_checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "30"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // 150 actions / 30 per part = 5 parts
        val actions: Seq[Action] = (1 to 150).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        writer.writeCheckpoint(version = 10L, actions = actions)

        // Verify _last_checkpoint content
        val infoOpt = reader.getLastCheckpointInfo()
        assert(infoOpt.isDefined)
        val info = infoOpt.get

        assert(info.version === 10L)
        assert(info.size === 150L)
        assert(info.numFiles === 150L)
        assert(info.isParquetFormat)
        assert(info.isMultiPart)
        assert(info.parts.contains(5), s"Expected 5 parts but got ${info.parts}")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("multi-part checkpoint should correctly calculate total size across all parts") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        val actions: Seq[Action] = (1 to 100).map { i =>
          AddAction(s"file$i.split", Map("partition" -> (i % 5).toString), i * 100L, System.currentTimeMillis(), true)
        }

        val metadata = writer.writeCheckpoint(version = 5L, actions = actions)

        // sizeInBytes should be > 0 and represent total across all parts
        assert(metadata.sizeInBytes > 0L, "Total size should be greater than 0")
        assert(metadata.parts.contains(2), "Should have 2 parts")
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Round-Robin Distribution
  // ==========================================================================

  test("distributeEntriesToParts should evenly distribute entries via round-robin") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Create 10 entries
        val entries = (1 to 10).map { i =>
          ParquetCheckpointEntry.fromAction(
            AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
          )
        }

        // Distribute to 3 parts
        val distributed = writer.distributeEntriesToParts(entries, 3)

        // Part 0: indices 0, 3, 6, 9 -> 4 entries (file1, file4, file7, file10)
        // Part 1: indices 1, 4, 7 -> 3 entries (file2, file5, file8)
        // Part 2: indices 2, 5, 8 -> 3 entries (file3, file6, file9)
        assert(distributed.length === 3)
        assert(distributed(0).length === 4, "Part 0 should have 4 entries")
        assert(distributed(1).length === 3, "Part 1 should have 3 entries")
        assert(distributed(2).length === 3, "Part 2 should have 3 entries")

        // Verify round-robin order
        assert(distributed(0).head.path.contains("file1"))
        assert(distributed(1).head.path.contains("file2"))
        assert(distributed(2).head.path.contains("file3"))
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("multi-part checkpoint should distribute all action types across parts") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "5"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create mixed action types
        val actions: Seq[Action] = Seq(
          ProtocolAction(1, 2),
          MetadataAction("id", Some("name"), None, FileFormat("p", Map.empty), "schema", Seq.empty, Map.empty, None)
        ) ++ (1 to 10).map { i =>
          AddAction(s"add$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        } ++ Seq(
          RemoveAction("remove1.split", Some(System.currentTimeMillis()), true, None, None, None),
          RemoveAction("remove2.split", Some(System.currentTimeMillis()), true, None, None, None)
        )

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        assert(metadata.parts.isDefined)
        assert(metadata.parts.get >= 2, "Should have multiple parts")

        // Read back and verify all action types are present
        val readActionsOpt = reader.readMultiPartCheckpoint(1L, metadata.parts.get)
        assert(readActionsOpt.isDefined)
        val readActions = readActionsOpt.get

        assert(readActions.count(_.isInstanceOf[ProtocolAction]) === 1)
        assert(readActions.count(_.isInstanceOf[MetadataAction]) === 1)
        assert(readActions.count(_.isInstanceOf[AddAction]) === 10)
        assert(readActions.count(_.isInstanceOf[RemoveAction]) === 2)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Read-Write Round-Trip
  // ==========================================================================

  test("multi-part checkpoint write then read round-trip preserves all actions") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "25"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create comprehensive test data with 100 actions (4 parts)
        val protocol = ProtocolAction(2, 3, Some(Set("feature1")), Some(Set("feature2")))
        val metadata = MetadataAction(
          id = "test-table-id",
          name = Some("Multi-Part Test"),
          description = Some("Testing multi-part checkpoints"),
          format = FileFormat("indextables", Map("opt1" -> "val1")),
          schemaString = """{"type":"struct","fields":[]}""",
          partitionColumns = Seq("date"),
          configuration = Map("conf1" -> "confVal1"),
          createdTime = Some(1705312000000L)
        )

        val addActions = (1 to 90).map { i =>
          AddAction(
            path = s"split-$i.split",
            partitionValues = Map("date" -> s"2024-01-${10 + (i % 20)}"),
            size = i * 1000L,
            modificationTime = 1705312000000L + i,
            dataChange = true,
            stats = Some(s"""{"numRecords":${i * 10}}"""),
            numRecords = Some(i * 10L),
            footerStartOffset = Some(i * 100L),
            footerEndOffset = Some(i * 1000L),
            hasFooterOffsets = true
          )
        }

        val removeActions = (1 to 8).map { i =>
          RemoveAction(
            path = s"removed-$i.split",
            deletionTimestamp = Some(1705312000000L + i),
            dataChange = true,
            extendedFileMetadata = Some(true),
            partitionValues = Some(Map("date" -> s"2024-01-0$i")),
            size = Some(i * 500L)
          )
        }

        val allActions: Seq[Action] = Seq(protocol, metadata) ++ addActions ++ removeActions

        // Write multi-part checkpoint
        val writeMetadata = writer.writeCheckpoint(version = 100L, actions = allActions)

        assert(writeMetadata.version === 100L)
        assert(writeMetadata.size === allActions.length)
        assert(writeMetadata.numFiles === 90L) // AddActions only
        assert(writeMetadata.isMultiPart)
        assert(writeMetadata.parts.contains(4), s"Expected 4 parts but got ${writeMetadata.parts}")

        // Read back using reader
        val readActionsOpt = reader.readMultiPartCheckpoint(100L, writeMetadata.parts.get)
        assert(readActionsOpt.isDefined)
        val readActions = readActionsOpt.get

        // Verify counts
        assert(readActions.length === allActions.length, "Total action count should match")
        assert(readActions.count(_.isInstanceOf[ProtocolAction]) === 1)
        assert(readActions.count(_.isInstanceOf[MetadataAction]) === 1)
        assert(readActions.count(_.isInstanceOf[AddAction]) === 90)
        assert(readActions.count(_.isInstanceOf[RemoveAction]) === 8)

        // Verify specific data is preserved
        val readProtocol = readActions.collectFirst { case p: ProtocolAction => p }.get
        assert(readProtocol.minReaderVersion === 2)
        assert(readProtocol.readerFeatures === Some(Set("feature1")))

        val readMeta = readActions.collectFirst { case m: MetadataAction => m }.get
        assert(readMeta.id === "test-table-id")
        assert(readMeta.name === Some("Multi-Part Test"))
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("multi-part checkpoint readCheckpointFromInfo correctly handles multi-part flag") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "20"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        val actions: Seq[Action] = (1 to 50).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        writer.writeCheckpoint(version = 5L, actions = actions)

        // Get info and use readCheckpointFromInfo
        val infoOpt = reader.getLastCheckpointInfo()
        assert(infoOpt.isDefined)
        val info = infoOpt.get
        assert(info.isMultiPart)
        assert(info.parts.contains(3), s"Expected 3 parts but got ${info.parts}")

        // Read using the info
        val readActionsOpt = reader.readCheckpointFromInfo(info)
        assert(readActionsOpt.isDefined)
        assert(readActionsOpt.get.length === 50)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Configuration
  // ==========================================================================

  test("multipart.enabled=false should create single file even for large action count") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "false",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Create 200 actions (would be 4 parts if enabled)
        val actions: Seq[Action] = (1 to 200).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        // Should be single-file checkpoint
        assert(!metadata.isMultiPart, "Should not be multi-part when disabled")
        assert(metadata.parts.isEmpty, "parts should be None")

        // Verify single file exists (not multi-part)
        val singleFilePath = writer.getCheckpointPath(1L)
        assert(cloudProvider.exists(singleFilePath.toString), "Single file checkpoint should exist")

        // Verify multi-part files don't exist
        val multiPartPath = writer.getMultiPartCheckpointPath(1L, 0)
        assert(!cloudProvider.exists(multiPartPath.toString), "Multi-part file should not exist")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("different maxActionsPerPart values produce correct part counts") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")

      // Test with 100 actions
      val actionCount = 100

      Seq(10, 25, 50, 100, 200).foreach { maxPerPart =>
        val options = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.multipart.enabled" -> "true",
          "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> maxPerPart.toString
        ).asJava)
        val cloudProvider = CloudStorageProviderFactory.createProvider(
          tempPath,
          options,
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val subPath = new Path(tempPath, s"test_$maxPerPart")
          val txLogPath = new Path(subPath, "_transaction_log")
          cloudProvider.createDirectory(txLogPath.toString)

          val writer = ParquetCheckpointWriter(txLogPath, cloudProvider, spark, options)

          val actions: Seq[Action] = (1 to actionCount).map { i =>
            AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
          }

          val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

          val expectedParts = if (actionCount <= maxPerPart) {
            None // Single file
          } else {
            Some(math.max(2, (actionCount + maxPerPart - 1) / maxPerPart))
          }

          assert(metadata.parts === expectedParts,
            s"With maxActionsPerPart=$maxPerPart, expected parts=$expectedParts but got ${metadata.parts}")
        } finally {
          cloudProvider.close()
        }
      }
    }
  }

  test("parallel vs sequential write should produce same result") {
    withTempPath { tempPath =>
      val actions: Seq[Action] = (1 to 100).map { i =>
        AddAction(
          path = s"file$i.split",
          partitionValues = Map("idx" -> i.toString),
          size = i * 100L,
          modificationTime = 1705312000000L + i,
          dataChange = true,
          numRecords = Some(i * 10L)
        )
      }

      // Write with parallel=true
      val parallelPath = new Path(tempPath, "parallel")
      val parallelTxLogPath = new Path(parallelPath, "_transaction_log")
      val parallelOptions = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "30",
        "spark.indextables.checkpoint.multipart.parallelWrite" -> "true"
      ).asJava)
      val parallelCloudProvider = CloudStorageProviderFactory.createProvider(
        parallelPath,
        parallelOptions,
        spark.sparkContext.hadoopConfiguration
      )

      // Write with parallel=false
      val sequentialPath = new Path(tempPath, "sequential")
      val sequentialTxLogPath = new Path(sequentialPath, "_transaction_log")
      val sequentialOptions = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "30",
        "spark.indextables.checkpoint.multipart.parallelWrite" -> "false"
      ).asJava)
      val sequentialCloudProvider = CloudStorageProviderFactory.createProvider(
        sequentialPath,
        sequentialOptions,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        parallelCloudProvider.createDirectory(parallelTxLogPath.toString)
        sequentialCloudProvider.createDirectory(sequentialTxLogPath.toString)

        val parallelWriter = ParquetCheckpointWriter(parallelTxLogPath, parallelCloudProvider, spark, parallelOptions)
        val sequentialWriter = ParquetCheckpointWriter(sequentialTxLogPath, sequentialCloudProvider, spark, sequentialOptions)

        val parallelReader = ParquetCheckpointReader(parallelTxLogPath, parallelCloudProvider, spark, parallelOptions)
        val sequentialReader = ParquetCheckpointReader(sequentialTxLogPath, sequentialCloudProvider, spark, sequentialOptions)

        val parallelMetadata = parallelWriter.writeCheckpoint(version = 1L, actions = actions)
        val sequentialMetadata = sequentialWriter.writeCheckpoint(version = 1L, actions = actions)

        // Same metadata (except possibly timing-related fields)
        assert(parallelMetadata.version === sequentialMetadata.version)
        assert(parallelMetadata.size === sequentialMetadata.size)
        assert(parallelMetadata.numFiles === sequentialMetadata.numFiles)
        assert(parallelMetadata.parts === sequentialMetadata.parts)

        // Read back and verify same content
        val parallelActions = parallelReader.readMultiPartCheckpoint(1L, parallelMetadata.parts.get).get
        val sequentialActions = sequentialReader.readMultiPartCheckpoint(1L, sequentialMetadata.parts.get).get

        assert(parallelActions.length === sequentialActions.length)
        assert(parallelActions.length === 100)
      } finally {
        parallelCloudProvider.close()
        sequentialCloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Edge Cases
  // ==========================================================================

  test("exactly maxActionsPerPart actions should create single file (no split)") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "100"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Create exactly 100 actions (equal to maxActionsPerPart)
        val actions: Seq[Action] = (1 to 100).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        // Should create single file (100 actions is NOT > 100)
        assert(!metadata.isMultiPart, "Should not be multi-part when exactly at threshold")
        assert(metadata.parts.isEmpty, "parts should be None for single-file checkpoint")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("maxActionsPerPart + 1 actions should create 2 parts") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "100"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Create 101 actions (one more than maxActionsPerPart)
        val actions: Seq[Action] = (1 to 101).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        // Should create 2 parts (minimum for multi-part)
        assert(metadata.isMultiPart, "Should be multi-part when over threshold")
        assert(metadata.parts.contains(2), s"Expected 2 parts but got ${metadata.parts}")

        // Verify both parts exist
        (0 until 2).foreach { part =>
          val partPath = writer.getMultiPartCheckpointPath(1L, part)
          assert(cloudProvider.exists(partPath.toString), s"Part $part should exist")
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("large checkpoint with many parts handles correctly") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create 500 actions (should create 10 parts)
        val actions: Seq[Action] = (1 to 500).map { i =>
          AddAction(
            path = s"large-file-$i.split",
            partitionValues = Map("partition" -> (i % 20).toString),
            size = i * 100L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 10L)
          )
        }

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        assert(metadata.parts.contains(10), s"Expected 10 parts but got ${metadata.parts}")
        assert(metadata.size === 500L)

        // Verify all 10 parts exist
        (0 until 10).foreach { part =>
          val partPath = writer.getMultiPartCheckpointPath(1L, part)
          assert(cloudProvider.exists(partPath.toString), s"Part $part should exist")
        }

        // Read back and verify all actions
        val readActionsOpt = reader.readMultiPartCheckpoint(1L, 10)
        assert(readActionsOpt.isDefined)
        assert(readActionsOpt.get.length === 500)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("empty actions list should create single-file checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Empty actions list
        val actions: Seq[Action] = Seq.empty

        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)

        // Should be single-file (no multi-part for empty)
        assert(!metadata.isMultiPart)
        assert(metadata.size === 0L)
        assert(metadata.numFiles === 0L)

        // Verify checkpoint file exists and is readable
        val readActionsOpt = reader.readParquetCheckpoint(1L)
        assert(readActionsOpt.isDefined)
        assert(readActionsOpt.get.isEmpty)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Error Handling
  // ==========================================================================

  test("validateMultiPartCheckpoint should catch entry count mismatch") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.validateOnWrite" -> "true",
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Write a valid checkpoint first
        val actions: Seq[Action] = (1 to 100).map { i =>
          AddAction(s"file$i.split", Map.empty, i * 100L, System.currentTimeMillis(), true)
        }

        // This should succeed
        val metadata = writer.writeCheckpoint(version = 1L, actions = actions)
        assert(metadata.parts.contains(2))

        // Now try to validate with wrong expected count (this would fail if called directly)
        val thrown = intercept[IllegalStateException] {
          writer.validateMultiPartCheckpoint(1L, 2, 999) // Wrong count
        }

        assert(thrown.getMessage.contains("expected 999 entries but found 100"))
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("validateMultiPartCheckpoint should detect missing part files") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.enabled" -> "true",
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "50"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // Try to validate non-existent parts
        val thrown = intercept[IllegalStateException] {
          writer.validateMultiPartCheckpoint(999L, 3, 100) // Version 999 doesn't exist
        }

        assert(thrown.getMessage.contains("missing parts"))
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - calculateNumParts
  // ==========================================================================

  test("calculateNumParts should return minimum 2 parts for multi-part checkpoints") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.multipart.maxActionsPerPart" -> "100"
      ).asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)

        // 101 actions would naturally calculate to ceil(101/100) = 2, which equals minimum
        assert(writer.calculateNumParts(101) === 2)

        // 150 actions would naturally calculate to ceil(150/100) = 2, which equals minimum
        assert(writer.calculateNumParts(150) === 2)

        // 201 actions would calculate to ceil(201/100) = 3
        assert(writer.calculateNumParts(201) === 3)

        // 500 actions would calculate to ceil(500/100) = 5
        assert(writer.calculateNumParts(500) === 5)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Multi-Part Checkpoint Tests - Reader Integration
  // ==========================================================================

  test("ParquetCheckpointReader should return None for missing multi-part checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Try to read non-existent multi-part checkpoint
        val result = reader.readMultiPartCheckpoint(999L, 3)
        assert(result.isEmpty, "Should return None for non-existent multi-part checkpoint")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader.readCheckpointFromInfo returns None for JSON format") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create a JSON format info
        val jsonInfo = LastCheckpointFileInfo(
          version = 10L,
          size = 100L,
          sizeInBytes = 5000L,
          numFiles = 80L,
          createdTime = System.currentTimeMillis(),
          format = Some("json"),
          parts = None
        )

        val result = reader.readCheckpointFromInfo(jsonInfo)
        assert(result.isEmpty, "Should return None for JSON format checkpoint info")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("Large number of actions in single checkpoint") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")
      val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        options,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        val writer = ParquetCheckpointWriter(transactionLogPath, cloudProvider, spark, options)
        val reader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, options)

        // Create a large number of actions
        val numActions = 1000
        val actions: Seq[Action] = (1 to numActions).map { i =>
          AddAction(
            path = s"large-batch-file-$i.split",
            partitionValues = Map("partition" -> (i % 10).toString),
            size = i * 1000L,
            modificationTime = System.currentTimeMillis() + i,
            dataChange = true,
            numRecords = Some(i * 10L),
            stats = Some(s"""{"numRecords":${i * 10}}""")
          )
        }

        val metadata = writer.writeCheckpoint(version = 1000L, actions = actions)

        assert(metadata.version === 1000L)
        assert(metadata.size === numActions)
        assert(metadata.numFiles === numActions)

        // Read back and verify
        val readActionsOpt = reader.readParquetCheckpoint(1000L)
        assert(readActionsOpt.isDefined)
        assert(readActionsOpt.get.length === numActions)
      } finally {
        cloudProvider.close()
      }
    }
  }

  // ==========================================================================
  // Mixed-Format Checkpoint Tests (JSON/Parquet Fallback)
  // ==========================================================================

  test("should read JSON checkpoint after enabling Parquet format (fallback)") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")

      // Step 1: Create table with JSON checkpoint format
      val jsonOptions = new CaseInsensitiveStringMap(Map(
        "spark.indextables.checkpoint.format" -> "json",
        "spark.indextables.checkpoint.interval" -> "3"
      ).asJava)

      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        jsonOptions,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Create JSON checkpoint using TransactionLogCheckpoint
        val jsonCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          jsonOptions,
          Some(spark)
        )

        val originalActions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction(
            id = "json-table",
            name = Some("JSON Format Table"),
            description = None,
            format = FileFormat("indextables", Map.empty),
            schemaString = """{"type":"struct","fields":[]}""",
            partitionColumns = Seq.empty,
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
        ) ++ (1 to 10).map { i =>
          AddAction(
            path = s"json-file-$i.split",
            partitionValues = Map("partition" -> (i % 3).toString),
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 100L)
          )
        }

        // Create JSON checkpoint at version 5
        jsonCheckpointer.createCheckpoint(5L, originalActions)
        jsonCheckpointer.close()

        // Verify JSON checkpoint file exists
        val jsonCheckpointPath = new Path(transactionLogPath, "00000000000000000005.checkpoint.json")
        assert(cloudProvider.exists(jsonCheckpointPath.toString), "JSON checkpoint file should exist")

        // Step 2: Now enable Parquet format and try to read
        val parquetOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "parquet",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava)

        val parquetCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          parquetOptions,
          Some(spark)
        )

        // Read using the Parquet-enabled checkpointer - should fall back to JSON
        val readActionsOpt = parquetCheckpointer.getActionsFromCheckpoint()
        parquetCheckpointer.close()

        // Step 3: Verify all data is present
        assert(readActionsOpt.isDefined, "Should successfully read JSON checkpoint with Parquet format enabled")
        val readActions = readActionsOpt.get

        assert(readActions.length === originalActions.length, "Action count should match")
        assert(readActions.count(_.isInstanceOf[ProtocolAction]) === 1)
        assert(readActions.count(_.isInstanceOf[MetadataAction]) === 1)
        assert(readActions.count(_.isInstanceOf[AddAction]) === 10)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("mixed JSON and Parquet checkpoints should coexist with latest Parquet taking precedence") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")

      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Step 1: Create JSON checkpoint at version N
        val jsonOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "json",
          "spark.indextables.checkpoint.interval" -> "5"
        ).asJava)

        val jsonCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          jsonOptions,
          Some(spark)
        )

        val jsonActions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction(
            id = "mixed-table",
            name = Some("Mixed Format Table"),
            description = None,
            format = FileFormat("indextables", Map.empty),
            schemaString = """{"type":"struct","fields":[]}""",
            partitionColumns = Seq.empty,
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
        ) ++ (1 to 5).map { i =>
          AddAction(
            path = s"json-era-file-$i.split",
            partitionValues = Map.empty,
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 100L)
          )
        }

        // Create JSON checkpoint at version 10
        jsonCheckpointer.createCheckpoint(10L, jsonActions)
        jsonCheckpointer.close()

        // Verify JSON checkpoint exists
        val jsonCheckpointPath = new Path(transactionLogPath, "00000000000000000010.checkpoint.json")
        assert(cloudProvider.exists(jsonCheckpointPath.toString), "JSON checkpoint should exist at version 10")

        // Step 2: Switch to Parquet format and write more data
        val parquetOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "parquet",
          "spark.indextables.checkpoint.interval" -> "5"
        ).asJava)

        val parquetCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          parquetOptions,
          Some(spark)
        )

        // Create new checkpoint with both old and new files (simulating accumulated state)
        val allActions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction(
            id = "mixed-table",
            name = Some("Mixed Format Table"),
            description = None,
            format = FileFormat("indextables", Map.empty),
            schemaString = """{"type":"struct","fields":[]}""",
            partitionColumns = Seq.empty,
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
        ) ++ (1 to 5).map { i =>
          AddAction(
            path = s"json-era-file-$i.split",
            partitionValues = Map.empty,
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 100L)
          )
        } ++ (1 to 5).map { i =>
          AddAction(
            path = s"parquet-era-file-$i.split",
            partitionValues = Map.empty,
            size = i * 2000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 200L)
          )
        }

        // Create Parquet checkpoint at version 20
        parquetCheckpointer.createCheckpoint(20L, allActions)
        parquetCheckpointer.close()

        // Verify Parquet checkpoint exists
        val parquetCheckpointsDir = new Path(transactionLogPath, "_checkpoints")
        val parquetCheckpointPath = new Path(parquetCheckpointsDir, "00000000000000000020.checkpoint.parquet")
        assert(cloudProvider.exists(parquetCheckpointPath.toString), "Parquet checkpoint should exist at version 20")

        // Both checkpoints should exist on disk
        assert(cloudProvider.exists(jsonCheckpointPath.toString), "Old JSON checkpoint should still exist")

        // Step 3: Read table - should use latest Parquet checkpoint
        val readerOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "parquet"
        ).asJava)

        val reader = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          readerOptions,
          Some(spark)
        )

        val readActionsOpt = reader.getActionsFromCheckpoint()
        reader.close()

        // Verify all data from Parquet checkpoint
        assert(readActionsOpt.isDefined, "Should read from Parquet checkpoint")
        val readActions = readActionsOpt.get

        assert(readActions.length === allActions.length, "Should have all actions from Parquet checkpoint")
        assert(readActions.count(_.isInstanceOf[AddAction]) === 10, "Should have 10 AddActions (5 old + 5 new)")

        // Verify we have files from both eras
        val addActions = readActions.collect { case a: AddAction => a }
        assert(addActions.exists(_.path.contains("json-era")), "Should have files from JSON era")
        assert(addActions.exists(_.path.contains("parquet-era")), "Should have files from Parquet era")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("format switching mid-session should create seamless transition") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")

      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Step 1: Write initial data with JSON format
        val jsonOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "json",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava)

        val jsonCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          jsonOptions,
          Some(spark)
        )

        // Verify format is JSON
        assert(jsonCheckpointer.getCheckpointFormat === "json", "Initial format should be JSON")

        val initialActions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction(
            id = "session-table",
            name = Some("Session Test Table"),
            description = None,
            format = FileFormat("indextables", Map.empty),
            schemaString = """{"type":"struct","fields":[]}""",
            partitionColumns = Seq("date"),
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
        ) ++ (1 to 3).map { i =>
          AddAction(
            path = s"initial-file-$i.split",
            partitionValues = Map("date" -> "2024-01-01"),
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 50L)
          )
        }

        jsonCheckpointer.createCheckpoint(5L, initialActions)
        jsonCheckpointer.close()

        // Verify JSON checkpoint created
        val jsonCheckpointPath = new Path(transactionLogPath, "00000000000000000005.checkpoint.json")
        assert(cloudProvider.exists(jsonCheckpointPath.toString), "JSON checkpoint should be created")

        // Step 2: Switch to Parquet format mid-session
        val parquetOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "parquet",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava)

        val parquetCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          parquetOptions,
          Some(spark)
        )

        // Verify format switched to Parquet
        assert(parquetCheckpointer.getCheckpointFormat === "parquet", "Format should now be Parquet")

        // Read existing data first (should fall back to JSON)
        val existingActions = parquetCheckpointer.getActionsFromCheckpoint()
        assert(existingActions.isDefined, "Should be able to read JSON checkpoint after format switch")
        assert(existingActions.get.length === initialActions.length, "Should have all initial actions")

        // Step 3: Write more data with Parquet format
        val updatedActions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          MetadataAction(
            id = "session-table",
            name = Some("Session Test Table"),
            description = None,
            format = FileFormat("indextables", Map.empty),
            schemaString = """{"type":"struct","fields":[]}""",
            partitionColumns = Seq("date"),
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
        ) ++ (1 to 3).map { i =>
          AddAction(
            path = s"initial-file-$i.split",
            partitionValues = Map("date" -> "2024-01-01"),
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 50L)
          )
        } ++ (1 to 3).map { i =>
          AddAction(
            path = s"updated-file-$i.split",
            partitionValues = Map("date" -> "2024-01-02"),
            size = i * 1500L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 75L)
          )
        }

        parquetCheckpointer.createCheckpoint(10L, updatedActions)
        parquetCheckpointer.close()

        // Verify Parquet checkpoint created
        val parquetCheckpointsDir = new Path(transactionLogPath, "_checkpoints")
        val parquetCheckpointPath = new Path(parquetCheckpointsDir, "00000000000000000010.checkpoint.parquet")
        assert(cloudProvider.exists(parquetCheckpointPath.toString), "Parquet checkpoint should be created")

        // Step 4: Verify seamless transition - read with a new reader
        val finalReader = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          parquetOptions,
          Some(spark)
        )

        val finalActionsOpt = finalReader.getActionsFromCheckpoint()
        finalReader.close()

        assert(finalActionsOpt.isDefined, "Should read final checkpoint")
        val finalActions = finalActionsOpt.get

        assert(finalActions.length === updatedActions.length, "Should have all updated actions")
        assert(finalActions.count(_.isInstanceOf[AddAction]) === 6, "Should have 6 AddActions")

        // Verify we have files from both dates
        val addActions = finalActions.collect { case a: AddAction => a }
        assert(addActions.exists(_.partitionValues.get("date").contains("2024-01-01")), "Should have files from first date")
        assert(addActions.exists(_.partitionValues.get("date").contains("2024-01-02")), "Should have files from second date")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("TransactionLogCheckpoint.getLastCheckpointInfo reads format field correctly") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")

      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Write a _last_checkpoint file with format field
        val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")

        // Test JSON format
        val jsonCheckpointContent = """{"version":5,"size":10,"sizeInBytes":5000,"numFiles":8,"createdTime":1705312000000,"format":"json"}"""
        cloudProvider.writeFile(lastCheckpointPath.toString, jsonCheckpointContent.getBytes("UTF-8"))

        val jsonOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "json"
        ).asJava)

        val jsonCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          jsonOptions,
          Some(spark)
        )

        val jsonInfoOpt = jsonCheckpointer.getLastCheckpointInfo()
        jsonCheckpointer.close()

        assert(jsonInfoOpt.isDefined)
        assert(jsonInfoOpt.get.version === 5L)
        assert(jsonInfoOpt.get.size === 10L)

        // Test Parquet format in _last_checkpoint
        val parquetCheckpointContent = """{"version":10,"size":20,"sizeInBytes":10000,"numFiles":15,"createdTime":1705312000000,"format":"parquet"}"""
        cloudProvider.writeFile(lastCheckpointPath.toString, parquetCheckpointContent.getBytes("UTF-8"))

        // Read with ParquetCheckpointReader to verify format detection
        val parquetOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "parquet"
        ).asJava)

        val parquetReader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, parquetOptions)
        val parquetInfoOpt = parquetReader.getLastCheckpointInfo()

        assert(parquetInfoOpt.isDefined)
        assert(parquetInfoOpt.get.version === 10L)
        assert(parquetInfoOpt.get.isParquetFormat)
        assert(!parquetInfoOpt.get.isJsonFormat)
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("ParquetCheckpointReader falls back gracefully when no Parquet checkpoint exists") {
    withTempPath { tempPath =>
      val transactionLogPath = new Path(tempPath, "_transaction_log")

      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Create a JSON checkpoint only
        val jsonOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "json"
        ).asJava)

        val jsonCheckpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          jsonOptions,
          Some(spark)
        )

        val actions: Seq[Action] = Seq(
          ProtocolAction(1, 1),
          AddAction("test-file.split", Map.empty, 1000L, System.currentTimeMillis(), true)
        )

        jsonCheckpointer.createCheckpoint(5L, actions)
        jsonCheckpointer.close()

        // Try to read with Parquet reader directly
        val parquetOptions = new CaseInsensitiveStringMap(Map(
          "spark.indextables.checkpoint.format" -> "parquet"
        ).asJava)

        val parquetReader = ParquetCheckpointReader(transactionLogPath, cloudProvider, spark, parquetOptions)

        // getActionsFromCheckpoint should return None (not Parquet format)
        val parquetActionsOpt = parquetReader.getActionsFromCheckpoint()
        assert(parquetActionsOpt.isEmpty, "ParquetCheckpointReader should return None for JSON checkpoint")

        // But TransactionLogCheckpoint should fall back to JSON
        val checkpointer = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          parquetOptions,
          Some(spark)
        )

        val fallbackActionsOpt = checkpointer.getActionsFromCheckpoint()
        checkpointer.close()

        assert(fallbackActionsOpt.isDefined, "TransactionLogCheckpoint should fall back to JSON checkpoint")
        assert(fallbackActionsOpt.get.length === 2, "Should have all actions from JSON checkpoint")
      } finally {
        cloudProvider.close()
      }
    }
  }
}
