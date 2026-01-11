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
}
