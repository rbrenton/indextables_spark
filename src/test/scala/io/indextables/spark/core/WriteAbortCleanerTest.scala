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

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.AddAction
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for WriteAbortCleaner, which handles cleanup of uncommitted files during write abort.
 */
class WriteAbortCleanerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var tempDir: File = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("write-abort-cleaner-test").toFile
    tempDir.mkdirs()
  }

  override def afterAll(): Unit =
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def createTestSplitFile(parentDir: File, filename: String, content: String = "test content"): File = {
    val file = new File(parentDir, filename)
    file.getParentFile.mkdirs()
    Files.write(file.toPath, content.getBytes)
    file
  }

  private def createAddAction(
    path: String,
    partitionValues: Map[String, String] = Map.empty,
    size: Long = 1000L
  ): AddAction =
    AddAction(
      path = path,
      partitionValues = partitionValues,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L)
    )

  // ============================================================================
  // SUCCESSFUL DELETION TESTS
  // ============================================================================

  test("should successfully delete multiple local files") {
    val testDir = new File(tempDir, "test-multiple-delete")
    testDir.mkdirs()

    // Create test split files
    val file1 = createTestSplitFile(testDir, "split1.split")
    val file2 = createTestSplitFile(testDir, "split2.split")
    val file3 = createTestSplitFile(testDir, "split3.split")

    assert(file1.exists(), "file1 should exist before cleanup")
    assert(file2.exists(), "file2 should exist before cleanup")
    assert(file3.exists(), "file3 should exist before cleanup")

    val addActions = Seq(
      createAddAction(file1.getAbsolutePath, size = 100L),
      createAddAction(file2.getAbsolutePath, size = 200L),
      createAddAction(file3.getAbsolutePath, size = 300L)
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    // Verify all files deleted
    assert(!file1.exists(), "file1 should be deleted")
    assert(!file2.exists(), "file2 should be deleted")
    assert(!file3.exists(), "file3 should be deleted")

    // Verify result counts
    result.deletedCount shouldBe 3
    result.failedCount shouldBe 0
    result.totalBytes shouldBe 600L // 100 + 200 + 300
  }

  test("should handle single file deletion") {
    val testDir = new File(tempDir, "test-single-delete")
    testDir.mkdirs()

    val file1 = createTestSplitFile(testDir, "single.split")
    assert(file1.exists(), "file should exist before cleanup")

    val addActions = Seq(createAddAction(file1.getAbsolutePath, size = 500L))

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file1.exists(), "file should be deleted")
    result.deletedCount shouldBe 1
    result.failedCount shouldBe 0
    result.totalBytes shouldBe 500L
  }

  // ============================================================================
  // EDGE CASE TESTS
  // ============================================================================

  test("should handle empty AddAction list") {
    val testDir = new File(tempDir, "test-empty-list")
    testDir.mkdirs()

    val addActions = Seq.empty[AddAction]

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    // Should return zeros and succeed
    result.deletedCount shouldBe 0
    result.failedCount shouldBe 0
    result.totalBytes shouldBe 0L
  }

  test("should handle files that do not exist (FileNotFoundException as success)") {
    val testDir = new File(tempDir, "test-nonexistent")
    testDir.mkdirs()

    // Create AddActions for files that don't exist
    val nonExistentPath1 = new File(testDir, "nonexistent1.split").getAbsolutePath
    val nonExistentPath2 = new File(testDir, "nonexistent2.split").getAbsolutePath

    // Verify files don't exist
    assert(!new File(nonExistentPath1).exists(), "nonexistent1 should not exist")
    assert(!new File(nonExistentPath2).exists(), "nonexistent2 should not exist")

    val addActions = Seq(
      createAddAction(nonExistentPath1, size = 100L),
      createAddAction(nonExistentPath2, size = 200L)
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    // FileNotFoundException should be treated as success (file already gone)
    result.deletedCount shouldBe 2
    result.failedCount shouldBe 0
  }

  test("should handle mixed existing and non-existing files") {
    val testDir = new File(tempDir, "test-mixed")
    testDir.mkdirs()

    // Create one real file
    val existingFile = createTestSplitFile(testDir, "existing.split")
    val nonExistentPath = new File(testDir, "nonexistent.split").getAbsolutePath

    assert(existingFile.exists(), "existing file should exist")
    assert(!new File(nonExistentPath).exists(), "nonexistent should not exist")

    val addActions = Seq(
      createAddAction(existingFile.getAbsolutePath, size = 100L),
      createAddAction(nonExistentPath, size = 200L)
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    // Both should be counted as deleted (existing deleted, nonexistent treated as success)
    assert(!existingFile.exists(), "existing file should be deleted")
    result.deletedCount shouldBe 2
    result.failedCount shouldBe 0
    result.totalBytes shouldBe 300L
  }

  // ============================================================================
  // RESULT COUNT ACCURACY TESTS
  // ============================================================================

  test("should verify cleanup result counts are accurate") {
    val testDir = new File(tempDir, "test-counts")
    testDir.mkdirs()

    // Create 5 test files
    val files = (1 to 5).map { i =>
      createTestSplitFile(testDir, s"split$i.split", s"content $i")
    }

    files.foreach(f => assert(f.exists(), s"${f.getName} should exist before cleanup"))

    val addActions = files.zipWithIndex.map { case (file, idx) =>
      createAddAction(file.getAbsolutePath, size = (idx + 1) * 100L)
    }

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    // All files should be deleted
    files.foreach(f => assert(!f.exists(), s"${f.getName} should be deleted"))

    // Counts should be accurate
    result.deletedCount shouldBe 5
    result.failedCount shouldBe 0
    result.totalBytes shouldBe 1500L // 100 + 200 + 300 + 400 + 500
  }

  test("should track bytes correctly for large files") {
    val testDir = new File(tempDir, "test-large-bytes")
    testDir.mkdirs()

    val file = createTestSplitFile(testDir, "large.split")

    // Use large file size in AddAction (actual file content doesn't matter for byte tracking)
    val largeSize = 5L * 1024L * 1024L * 1024L // 5GB simulated
    val addActions = Seq(createAddAction(file.getAbsolutePath, size = largeSize))

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    result.deletedCount shouldBe 1
    result.totalBytes shouldBe largeSize
  }

  // ============================================================================
  // PARTITIONED PATH TESTS
  // ============================================================================

  test("should handle partitioned paths (year=2024/month=01/file.split)") {
    val testDir = new File(tempDir, "test-partitioned")
    testDir.mkdirs()

    // Create nested partition directory structure
    val partitionDir = new File(testDir, "year=2024/month=01")
    partitionDir.mkdirs()

    val file = createTestSplitFile(partitionDir, "data.split")
    assert(file.exists(), "partitioned file should exist")

    val addActions = Seq(
      createAddAction(
        file.getAbsolutePath,
        partitionValues = Map("year" -> "2024", "month" -> "01"),
        size = 1000L
      )
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file.exists(), "partitioned file should be deleted")
    result.deletedCount shouldBe 1
    result.failedCount shouldBe 0
  }

  test("should handle multiple files in different partitions") {
    val testDir = new File(tempDir, "test-multi-partition")
    testDir.mkdirs()

    // Create files in different partition paths
    val partition1 = new File(testDir, "year=2024/month=01")
    val partition2 = new File(testDir, "year=2024/month=02")
    val partition3 = new File(testDir, "year=2023/month=12")

    val file1 = createTestSplitFile(partition1, "split1.split")
    val file2 = createTestSplitFile(partition2, "split2.split")
    val file3 = createTestSplitFile(partition3, "split3.split")

    assert(file1.exists() && file2.exists() && file3.exists(), "all files should exist")

    val addActions = Seq(
      createAddAction(
        file1.getAbsolutePath,
        partitionValues = Map("year" -> "2024", "month" -> "01"),
        size = 100L
      ),
      createAddAction(
        file2.getAbsolutePath,
        partitionValues = Map("year" -> "2024", "month" -> "02"),
        size = 200L
      ),
      createAddAction(
        file3.getAbsolutePath,
        partitionValues = Map("year" -> "2023", "month" -> "12"),
        size = 300L
      )
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file1.exists() && !file2.exists() && !file3.exists(), "all files should be deleted")
    result.deletedCount shouldBe 3
    result.failedCount shouldBe 0
    result.totalBytes shouldBe 600L
  }

  test("should handle deeply nested partition paths") {
    val testDir = new File(tempDir, "test-deep-partition")
    testDir.mkdirs()

    // Create deeply nested partition structure
    val deepPath = new File(testDir, "region=us-east/year=2024/month=01/day=15/hour=12")
    deepPath.mkdirs()

    val file = createTestSplitFile(deepPath, "deep.split")
    assert(file.exists(), "deeply nested file should exist")

    val addActions = Seq(
      createAddAction(
        file.getAbsolutePath,
        partitionValues = Map(
          "region" -> "us-east",
          "year"   -> "2024",
          "month"  -> "01",
          "day"    -> "15",
          "hour"   -> "12"
        ),
        size = 500L
      )
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file.exists(), "deeply nested file should be deleted")
    result.deletedCount shouldBe 1
    result.failedCount shouldBe 0
  }

  // ============================================================================
  // RELATIVE PATH RESOLUTION TESTS
  // ============================================================================

  test("should resolve relative paths correctly") {
    val testDir = new File(tempDir, "test-relative")
    testDir.mkdirs()

    // Create file with partition structure
    val partitionDir = new File(testDir, "date=2024-01-01")
    val file = createTestSplitFile(partitionDir, "data.split")
    assert(file.exists(), "file should exist")

    // Use relative path in AddAction (like Delta Lake does)
    val relativePath = "date=2024-01-01/data.split"
    val addActions = Seq(
      createAddAction(
        relativePath,
        partitionValues = Map("date" -> "2024-01-01"),
        size = 1000L
      )
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file.exists(), "file referenced by relative path should be deleted")
    result.deletedCount shouldBe 1
    result.failedCount shouldBe 0
  }

  test("should handle mixed relative and absolute paths") {
    val testDir = new File(tempDir, "test-mixed-paths")
    testDir.mkdirs()

    val partitionDir = new File(testDir, "partition=a")
    val file1 = createTestSplitFile(partitionDir, "relative.split")
    val file2 = createTestSplitFile(partitionDir, "absolute.split")

    val addActions = Seq(
      createAddAction("partition=a/relative.split", size = 100L), // Relative path
      createAddAction(file2.getAbsolutePath, size = 200L)         // Absolute path
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file1.exists() && !file2.exists(), "both files should be deleted")
    result.deletedCount shouldBe 2
    result.failedCount shouldBe 0
  }

  // ============================================================================
  // SERIALIZED OPTIONS OVERLOAD TESTS
  // ============================================================================

  test("should work with serialized options overload") {
    val testDir = new File(tempDir, "test-serialized")
    testDir.mkdirs()

    val file = createTestSplitFile(testDir, "serialized.split")
    assert(file.exists(), "file should exist")

    val addActions = Seq(createAddAction(file.getAbsolutePath, size = 1000L))

    val tablePath = new Path(testDir.getAbsolutePath)
    val serializedOptions = Map("some.option" -> "value")
    val serializedHadoopConf = Map(
      "fs.file.impl"                                    -> "org.apache.hadoop.fs.LocalFileSystem",
      "fs.file.impl.disable.cache"                      -> "true"
    )

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      serializedOptions,
      serializedHadoopConf
    )

    assert(!file.exists(), "file should be deleted using serialized overload")
    result.deletedCount shouldBe 1
    result.failedCount shouldBe 0
  }

  // ============================================================================
  // SPECIAL CHARACTER TESTS
  // ============================================================================

  test("should handle partition values with special characters") {
    val testDir = new File(tempDir, "test-special-chars")
    testDir.mkdirs()

    // Create partition with special characters (encoded)
    val partitionDir = new File(testDir, "region=us-east-1")
    val file = createTestSplitFile(partitionDir, "data.split")

    val addActions = Seq(
      createAddAction(
        file.getAbsolutePath,
        partitionValues = Map("region" -> "us-east-1"),
        size = 100L
      )
    )

    val tablePath = new Path(testDir.getAbsolutePath)
    val hadoopConf = new Configuration()

    val result = WriteAbortCleaner.cleanupUncommittedFiles(
      addActions,
      tablePath,
      Map.empty[String, String],
      hadoopConf
    )

    assert(!file.exists(), "file with special chars in partition should be deleted")
    result.deletedCount shouldBe 1
    result.failedCount shouldBe 0
  }
}
