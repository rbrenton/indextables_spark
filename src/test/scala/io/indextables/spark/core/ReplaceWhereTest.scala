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

import org.apache.spark.sql.{DataFrame, SaveMode}

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.TransactionLogFactory

/**
 * Comprehensive tests for the ReplaceWhere feature.
 *
 * ReplaceWhere enables selective partition overwrite based on a predicate, allowing users to replace data in specific
 * partitions without affecting other partitions.
 */
class ReplaceWhereTest extends TestBase {

  private val PROVIDER = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ============================================================================
  // BASIC REPLACEWHERE TESTS
  // ============================================================================

  test("replaceWhere should replace files matching single partition equality predicate") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Step 1: Create initial partitioned data with 3 dates
      val initialData = Seq(
        (1, "Alice", "2024-01-01"),
        (2, "Bob", "2024-01-01"),
        (3, "Charlie", "2024-01-02"),
        (4, "Diana", "2024-01-02"),
        (5, "Eve", "2024-01-03")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Verify initial state
      val beforeReplace = spark.read.format(PROVIDER).load(tablePath)
      assert(beforeReplace.count() == 5)

      // Step 2: Replace data for date = '2024-01-01' only
      val newData = Seq(
        (10, "NewAlice", "2024-01-01"),
        (11, "NewBob", "2024-01-01"),
        (12, "NewCharlie", "2024-01-01")
      ).toDF("id", "name", "date")

      newData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date = '2024-01-01'")
        .partitionBy("date")
        .save(tablePath)

      // Step 3: Verify results
      val result = spark.read.format(PROVIDER).load(tablePath).orderBy("id")

      // Should have 3 new records for 2024-01-01, plus 3 preserved records for other dates
      assert(result.count() == 6)

      // Verify 2024-01-01 partition has new data
      val date01 = result.filter($"date" === "2024-01-01")
      assert(date01.count() == 3)
      assert(date01.filter($"name".startsWith("New")).count() == 3)

      // Verify other partitions are preserved
      val date02 = result.filter($"date" === "2024-01-02")
      assert(date02.count() == 2)
      assert(date02.filter($"name" === "Charlie" || $"name" === "Diana").count() == 2)

      val date03 = result.filter($"date" === "2024-01-03")
      assert(date03.count() == 1)
      assert(date03.filter($"name" === "Eve").count() == 1)
    }
  }

  test("replaceWhere should handle IN clause predicate") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data with 4 dates
      val initialData = Seq(
        (1, "A", "2024-01-01"),
        (2, "B", "2024-01-02"),
        (3, "C", "2024-01-03"),
        (4, "D", "2024-01-04")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace data for dates IN ('2024-01-01', '2024-01-02')
      val newData = Seq(
        (10, "NewA", "2024-01-01"),
        (11, "NewB", "2024-01-02")
      ).toDF("id", "name", "date")

      newData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date IN ('2024-01-01', '2024-01-02')")
        .partitionBy("date")
        .save(tablePath)

      // Verify results
      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 4)

      // Replaced partitions should have new data
      assert(result.filter($"date" === "2024-01-01" && $"name" === "NewA").count() == 1)
      assert(result.filter($"date" === "2024-01-02" && $"name" === "NewB").count() == 1)

      // Non-replaced partitions should have original data
      assert(result.filter($"date" === "2024-01-03" && $"name" === "C").count() == 1)
      assert(result.filter($"date" === "2024-01-04" && $"name" === "D").count() == 1)
    }
  }

  test("replaceWhere should handle range predicate with greater-than-or-equal") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data with 5 dates
      val initialData = Seq(
        (1, "A", "2024-01-01"),
        (2, "B", "2024-01-02"),
        (3, "C", "2024-01-03"),
        (4, "D", "2024-01-04"),
        (5, "E", "2024-01-05")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace all data for date >= '2024-01-03'
      val newData = Seq(
        (30, "New-03", "2024-01-03"),
        (40, "New-04", "2024-01-04"),
        (50, "New-05", "2024-01-05")
      ).toDF("id", "name", "date")

      newData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date >= '2024-01-03'")
        .partitionBy("date")
        .save(tablePath)

      // Verify results
      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 5)

      // Dates before 2024-01-03 should have original data
      assert(result.filter($"date" === "2024-01-01" && $"name" === "A").count() == 1)
      assert(result.filter($"date" === "2024-01-02" && $"name" === "B").count() == 1)

      // Dates >= 2024-01-03 should have new data
      assert(result.filter($"date" === "2024-01-03" && $"name" === "New-03").count() == 1)
      assert(result.filter($"date" === "2024-01-04" && $"name" === "New-04").count() == 1)
      assert(result.filter($"date" === "2024-01-05" && $"name" === "New-05").count() == 1)
    }
  }

  test("replaceWhere should handle compound AND predicate with multiple partition columns") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data with year and month partitions
      val initialData = Seq(
        (1, "A", "2024", "01"),
        (2, "B", "2024", "01"),
        (3, "C", "2024", "02"),
        (4, "D", "2024", "02"),
        (5, "E", "2025", "01"),
        (6, "F", "2025", "02")
      ).toDF("id", "name", "year", "month")

      initialData.write
        .format(PROVIDER)
        .partitionBy("year", "month")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace only year = '2024' AND month = '01'
      val newData = Seq(
        (100, "NewA", "2024", "01"),
        (101, "NewB", "2024", "01"),
        (102, "NewC", "2024", "01")
      ).toDF("id", "name", "year", "month")

      newData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "year = '2024' AND month = '01'")
        .partitionBy("year", "month")
        .save(tablePath)

      // Verify results
      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 7) // 3 new + 4 preserved

      // Target partition should have new data
      val target = result.filter($"year" === "2024" && $"month" === "01")
      assert(target.count() == 3)
      assert(target.filter($"name".startsWith("New")).count() == 3)

      // Other partitions should be preserved
      assert(result.filter($"year" === "2024" && $"month" === "02").count() == 2)
      assert(result.filter($"year" === "2025" && $"month" === "01").count() == 1)
      assert(result.filter($"year" === "2025" && $"month" === "02").count() == 1)
    }
  }

  // ============================================================================
  // DATA INTEGRITY TESTS
  // ============================================================================

  test("replaceWhere should only remove files matching predicate - verify transaction log") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "part1"),
        (2, "B", "part2"),
        (3, "C", "part3")
      ).toDF("id", "name", "partition_key")

      initialData.write
        .format(PROVIDER)
        .partitionBy("partition_key")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Get initial file count from transaction log
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
      try {
        val initialFiles = transactionLog.listFiles()
        assert(initialFiles.nonEmpty)

        // Replace only partition_key = 'part1'
        val newData = Seq((10, "NewA", "part1")).toDF("id", "name", "partition_key")

        newData.write
          .format(PROVIDER)
          .mode(SaveMode.Overwrite)
          .option("replaceWhere", "partition_key = 'part1'")
          .partitionBy("partition_key")
          .save(tablePath)

        // Verify transaction log state
        val finalFiles = transactionLog.listFiles()

        // Should have files for all 3 partitions
        val part1Files = finalFiles.filter(_.partitionValues.get("partition_key").contains("part1"))
        val part2Files = finalFiles.filter(_.partitionValues.get("partition_key").contains("part2"))
        val part3Files = finalFiles.filter(_.partitionValues.get("partition_key").contains("part3"))

        assert(part1Files.nonEmpty, "part1 should have new files")
        assert(part2Files.nonEmpty, "part2 should be preserved")
        assert(part3Files.nonEmpty, "part3 should be preserved")

        // Verify data integrity
        val result = spark.read.format(PROVIDER).load(tablePath)
        assert(result.count() == 3)
        assert(result.filter($"id" === 10 && $"partition_key" === "part1").count() == 1)
        assert(result.filter($"id" === 2 && $"partition_key" === "part2").count() == 1)
        assert(result.filter($"id" === 3 && $"partition_key" === "part3").count() == 1)
      } finally {
        transactionLog.close()
      }
    }
  }

  test("replaceWhere transaction log should contain REMOVE and ADD actions") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "target"),
        (2, "B", "other")
      ).toDF("id", "name", "partition_key")

      initialData.write
        .format(PROVIDER)
        .partitionBy("partition_key")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Perform replaceWhere
      val newData = Seq(
        (10, "NewA", "target"),
        (11, "NewB", "target")
      ).toDF("id", "name", "partition_key")

      newData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "partition_key = 'target'")
        .partitionBy("partition_key")
        .save(tablePath)

      // Use DESCRIBE TRANSACTION LOG to verify actions
      val describeResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' INCLUDE ALL").collect()

      // Should have both add and remove actions for the replaceWhere version
      val removeActions = describeResult.filter(_.getString(2) == "remove")
      val addActions = describeResult.filter(_.getString(2) == "add")

      assert(removeActions.nonEmpty, "Should have remove actions for replaced partition")
      assert(addActions.nonEmpty, "Should have add actions for new data")

      // Verify remove action is for the correct partition
      val removedPath = removeActions.find(row => {
        val path = row.getString(3)
        path != null && path.contains("partition_key=target")
      })
      assert(removedPath.isDefined, "Remove action should be for 'target' partition")
    }
  }

  // ============================================================================
  // ERROR HANDLING TESTS
  // ============================================================================

  test("replaceWhere should throw exception on non-partitioned table") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create non-partitioned data
      val initialData = Seq(
        (1, "Alice"),
        (2, "Bob")
      ).toDF("id", "name")

      initialData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Attempt replaceWhere on non-partitioned table should fail
      val newData = Seq((10, "NewAlice")).toDF("id", "name")

      val exception = intercept[Exception] {
        newData.write
          .format(PROVIDER)
          .mode(SaveMode.Overwrite)
          .option("replaceWhere", "id = 1")
          .save(tablePath)
      }

      assert(
        exception.getMessage.contains("non-partitioned") ||
          exception.getMessage.contains("no partition columns"),
        s"Expected error about non-partitioned table, got: ${exception.getMessage}"
      )
    }
  }

  test("replaceWhere should throw exception when predicate references non-partition column") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create partitioned data
      val initialData = Seq(
        (1, "Alice", "2024-01-01"),
        (2, "Bob", "2024-01-02")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Attempt replaceWhere with non-partition column should fail
      val newData = Seq((10, "NewAlice", "2024-01-01")).toDF("id", "name", "date")

      val exception = intercept[Exception] {
        newData.write
          .format(PROVIDER)
          .mode(SaveMode.Overwrite)
          .option("replaceWhere", "name = 'Alice'")
          .partitionBy("date")
          .save(tablePath)
      }

      assert(
        exception.getMessage.contains("non-partition") ||
          exception.getMessage.contains("Only partition columns"),
        s"Expected error about non-partition column, got: ${exception.getMessage}"
      )
    }
  }

  test("replaceWhere should throw exception for invalid predicate syntax") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create partitioned data
      val initialData = Seq(
        (1, "Alice", "2024-01-01")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Attempt replaceWhere with invalid syntax
      val newData = Seq((10, "NewAlice", "2024-01-01")).toDF("id", "name", "date")

      val exception = intercept[Exception] {
        newData.write
          .format(PROVIDER)
          .mode(SaveMode.Overwrite)
          .option("replaceWhere", "date === '2024-01-01'") // Invalid: triple equals
          .partitionBy("date")
          .save(tablePath)
      }

      assert(exception != null, "Should throw exception for invalid predicate syntax")
    }
  }

  test("replaceWhere should throw exception for empty predicate") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create partitioned data
      val initialData = Seq(
        (1, "Alice", "2024-01-01")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Attempt replaceWhere with empty predicate
      val newData = Seq((10, "NewAlice", "2024-01-01")).toDF("id", "name", "date")

      val exception = intercept[Exception] {
        newData.write
          .format(PROVIDER)
          .mode(SaveMode.Overwrite)
          .option("replaceWhere", "")
          .partitionBy("date")
          .save(tablePath)
      }

      assert(exception != null, "Should throw exception for empty predicate")
    }
  }

  // ============================================================================
  // INTEGRATION TESTS
  // ============================================================================

  test("multiple sequential replaceWhere operations should work correctly") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "p1"),
        (2, "B", "p2"),
        (3, "C", "p3"),
        (4, "D", "p4")
      ).toDF("id", "name", "partition")

      initialData.write
        .format(PROVIDER)
        .partitionBy("partition")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // First replaceWhere: p1
      Seq((10, "NewA", "p1")).toDF("id", "name", "partition").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "partition = 'p1'")
        .partitionBy("partition")
        .save(tablePath)

      var result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.filter($"partition" === "p1" && $"id" === 10).count() == 1)
      assert(result.filter($"partition" === "p2" && $"id" === 2).count() == 1)

      // Second replaceWhere: p2
      Seq((20, "NewB", "p2")).toDF("id", "name", "partition").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "partition = 'p2'")
        .partitionBy("partition")
        .save(tablePath)

      result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.filter($"partition" === "p1" && $"id" === 10).count() == 1)
      assert(result.filter($"partition" === "p2" && $"id" === 20).count() == 1)
      assert(result.filter($"partition" === "p3" && $"id" === 3).count() == 1)

      // Third replaceWhere: p3 and p4 together
      Seq(
        (30, "NewC", "p3"),
        (40, "NewD", "p4")
      ).toDF("id", "name", "partition").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "partition IN ('p3', 'p4')")
        .partitionBy("partition")
        .save(tablePath)

      result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 4)
      assert(result.filter($"id" === 10).count() == 1)
      assert(result.filter($"id" === 20).count() == 1)
      assert(result.filter($"id" === 30).count() == 1)
      assert(result.filter($"id" === 40).count() == 1)
    }
  }

  test("replaceWhere followed by append should work correctly") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "2024-01-01"),
        (2, "B", "2024-01-02")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // ReplaceWhere for 2024-01-01
      Seq((10, "NewA", "2024-01-01")).toDF("id", "name", "date").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date = '2024-01-01'")
        .partitionBy("date")
        .save(tablePath)

      // Append new data to a new partition
      Seq((3, "C", "2024-01-03")).toDF("id", "name", "date").write
        .format(PROVIDER)
        .mode(SaveMode.Append)
        .partitionBy("date")
        .save(tablePath)

      // Verify all data
      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 3)
      assert(result.filter($"date" === "2024-01-01" && $"id" === 10).count() == 1)
      assert(result.filter($"date" === "2024-01-02" && $"id" === 2).count() == 1)
      assert(result.filter($"date" === "2024-01-03" && $"id" === 3).count() == 1)
    }
  }

  test("append followed by replaceWhere should work correctly") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "2024-01-01"),
        (2, "B", "2024-01-02")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Append more data to 2024-01-01
      Seq((3, "C", "2024-01-01")).toDF("id", "name", "date").write
        .format(PROVIDER)
        .mode(SaveMode.Append)
        .partitionBy("date")
        .save(tablePath)

      // Verify append worked
      var result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.filter($"date" === "2024-01-01").count() == 2) // A and C

      // ReplaceWhere for 2024-01-01 should replace ALL data in that partition
      Seq((10, "Replacement", "2024-01-01")).toDF("id", "name", "date").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date = '2024-01-01'")
        .partitionBy("date")
        .save(tablePath)

      // Verify replaceWhere removed both original and appended data
      result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 2)
      assert(result.filter($"date" === "2024-01-01").count() == 1)
      assert(result.filter($"date" === "2024-01-01" && $"name" === "Replacement").count() == 1)
      assert(result.filter($"date" === "2024-01-02" && $"id" === 2).count() == 1)
    }
  }

  test("replaceWhere with three partition columns") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create data with year/month/day partitions
      val initialData = Seq(
        (1, "A", "2024", "01", "01"),
        (2, "B", "2024", "01", "02"),
        (3, "C", "2024", "02", "01"),
        (4, "D", "2025", "01", "01")
      ).toDF("id", "name", "year", "month", "day")

      initialData.write
        .format(PROVIDER)
        .partitionBy("year", "month", "day")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace specific partition: year='2024' AND month='01' AND day='01'
      Seq((100, "NewA", "2024", "01", "01")).toDF("id", "name", "year", "month", "day").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "year = '2024' AND month = '01' AND day = '01'")
        .partitionBy("year", "month", "day")
        .save(tablePath)

      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 4)
      assert(result.filter($"year" === "2024" && $"month" === "01" && $"day" === "01" && $"id" === 100).count() == 1)
      assert(result.filter($"year" === "2024" && $"month" === "01" && $"day" === "02" && $"id" === 2).count() == 1)
      assert(result.filter($"year" === "2024" && $"month" === "02" && $"day" === "01" && $"id" === 3).count() == 1)
      assert(result.filter($"year" === "2025" && $"month" === "01" && $"day" === "01" && $"id" === 4).count() == 1)
    }
  }

  // ============================================================================
  // EDGE CASE TESTS
  // ============================================================================

  test("replaceWhere when no files match the predicate should just add new data") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "2024-01-01"),
        (2, "B", "2024-01-02")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // ReplaceWhere for a partition that doesn't exist
      Seq((10, "NewData", "2024-01-10")).toDF("id", "name", "date").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date = '2024-01-10'")
        .partitionBy("date")
        .save(tablePath)

      // All original data should be preserved, plus new data added
      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 3)
      assert(result.filter($"date" === "2024-01-01" && $"id" === 1).count() == 1)
      assert(result.filter($"date" === "2024-01-02" && $"id" === 2).count() == 1)
      assert(result.filter($"date" === "2024-01-10" && $"id" === 10).count() == 1)
    }
  }

  test("replaceWhere with empty new data should delete matching partition data") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "2024-01-01"),
        (2, "B", "2024-01-01"),
        (3, "C", "2024-01-02")
      ).toDF("id", "name", "date")

      initialData.write
        .format(PROVIDER)
        .partitionBy("date")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      assert(spark.read.format(PROVIDER).load(tablePath).count() == 3)

      // ReplaceWhere with empty DataFrame for date = '2024-01-01'
      // This should delete all data in that partition
      val emptyDf = spark.emptyDataFrame
        .withColumn("id", org.apache.spark.sql.functions.lit(null).cast("int"))
        .withColumn("name", org.apache.spark.sql.functions.lit(null).cast("string"))
        .withColumn("date", org.apache.spark.sql.functions.lit(null).cast("string"))
        .filter(org.apache.spark.sql.functions.lit(false)) // Ensure empty

      emptyDf.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "date = '2024-01-01'")
        .partitionBy("date")
        .save(tablePath)

      // Should only have 2024-01-02 data remaining
      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 1)
      assert(result.filter($"date" === "2024-01-01").count() == 0)
      assert(result.filter($"date" === "2024-01-02" && $"id" === 3).count() == 1)
    }
  }

  test("replaceWhere with special characters in partition values") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create data with special characters in partition values
      val initialData = Seq(
        (1, "A", "us-east-1"),
        (2, "B", "eu-west-2"),
        (3, "C", "ap-south-1")
      ).toDF("id", "name", "region")

      initialData.write
        .format(PROVIDER)
        .partitionBy("region")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace partition with hyphen in value
      Seq((10, "NewA", "us-east-1")).toDF("id", "name", "region").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "region = 'us-east-1'")
        .partitionBy("region")
        .save(tablePath)

      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 3)
      assert(result.filter($"region" === "us-east-1" && $"id" === 10).count() == 1)
      assert(result.filter($"region" === "eu-west-2" && $"id" === 2).count() == 1)
      assert(result.filter($"region" === "ap-south-1" && $"id" === 3).count() == 1)
    }
  }

  test("replaceWhere with OR predicate") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", "p1"),
        (2, "B", "p2"),
        (3, "C", "p3"),
        (4, "D", "p4")
      ).toDF("id", "name", "partition")

      initialData.write
        .format(PROVIDER)
        .partitionBy("partition")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace p1 OR p3
      Seq(
        (10, "NewA", "p1"),
        (30, "NewC", "p3")
      ).toDF("id", "name", "partition").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "partition = 'p1' OR partition = 'p3'")
        .partitionBy("partition")
        .save(tablePath)

      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 4)
      assert(result.filter($"partition" === "p1" && $"id" === 10).count() == 1)
      assert(result.filter($"partition" === "p2" && $"id" === 2).count() == 1)
      assert(result.filter($"partition" === "p3" && $"id" === 30).count() == 1)
      assert(result.filter($"partition" === "p4" && $"id" === 4).count() == 1)
    }
  }

  test("replaceWhere with less-than predicate") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data with numeric-like strings
      val initialData = Seq(
        (1, "A", "2024-01"),
        (2, "B", "2024-02"),
        (3, "C", "2024-03"),
        (4, "D", "2024-04")
      ).toDF("id", "name", "month")

      initialData.write
        .format(PROVIDER)
        .partitionBy("month")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace months < '2024-03'
      Seq(
        (10, "NewA", "2024-01"),
        (20, "NewB", "2024-02")
      ).toDF("id", "name", "month").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "month < '2024-03'")
        .partitionBy("month")
        .save(tablePath)

      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 4)
      assert(result.filter($"month" === "2024-01" && $"id" === 10).count() == 1)
      assert(result.filter($"month" === "2024-02" && $"id" === 20).count() == 1)
      assert(result.filter($"month" === "2024-03" && $"id" === 3).count() == 1) // Original preserved
      assert(result.filter($"month" === "2024-04" && $"id" === 4).count() == 1) // Original preserved
    }
  }

  test("replaceWhere with BETWEEN predicate") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "A", 1),
        (2, "B", 2),
        (3, "C", 3),
        (4, "D", 4),
        (5, "E", 5)
      ).toDF("id", "name", "level")

      initialData.write
        .format(PROVIDER)
        .partitionBy("level")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Replace levels BETWEEN 2 AND 4
      Seq(
        (20, "NewB", 2),
        (30, "NewC", 3),
        (40, "NewD", 4)
      ).toDF("id", "name", "level").write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "level BETWEEN 2 AND 4")
        .partitionBy("level")
        .save(tablePath)

      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 5)
      assert(result.filter($"level" === 1 && $"id" === 1).count() == 1) // Original
      assert(result.filter($"level" === 2 && $"id" === 20).count() == 1) // Replaced
      assert(result.filter($"level" === 3 && $"id" === 30).count() == 1) // Replaced
      assert(result.filter($"level" === 4 && $"id" === 40).count() == 1) // Replaced
      assert(result.filter($"level" === 5 && $"id" === 5).count() == 1) // Original
    }
  }

  test("replaceWhere should handle large number of records per partition") {
    withTempPath { tempPath =>
      val tablePath = tempPath
      val sparkSession = spark
      import sparkSession.implicits._

      // Create initial data with 1000 records per partition
      val initialData = (1 to 2000).map { i =>
        val partition = if (i <= 1000) "part1" else "part2"
        (i, s"Name$i", partition)
      }.toDF("id", "name", "partition")

      initialData.write
        .format(PROVIDER)
        .partitionBy("partition")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      assert(spark.read.format(PROVIDER).load(tablePath).count() == 2000)

      // Replace part1 with new data
      val newData = (10001 to 10500).map { i =>
        (i, s"NewName$i", "part1")
      }.toDF("id", "name", "partition")

      newData.write
        .format(PROVIDER)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", "partition = 'part1'")
        .partitionBy("partition")
        .save(tablePath)

      val result = spark.read.format(PROVIDER).load(tablePath)
      assert(result.count() == 1500) // 500 new + 1000 preserved
      assert(result.filter($"partition" === "part1").count() == 500)
      assert(result.filter($"partition" === "part2").count() == 1000)
    }
  }

  // ============================================================================
  // TRANSACTION LOG UNIT TESTS
  // ============================================================================

  test("TransactionLog.replaceWhere should atomically replace matching files") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        import io.indextables.spark.transaction.AddAction
        import org.apache.spark.sql.types._

        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("date", StringType)
        ))

        // Initialize with partition columns
        transactionLog.initialize(schema, Seq("date"))

        // Add initial files
        val initialActions = Seq(
          AddAction(
            path = "date=2024-01-01/file1.split",
            partitionValues = Map("date" -> "2024-01-01"),
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          ),
          AddAction(
            path = "date=2024-01-02/file2.split",
            partitionValues = Map("date" -> "2024-01-02"),
            size = 2000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(200L)
          )
        )
        transactionLog.addFiles(initialActions)

        // Verify initial state
        val filesBefore = transactionLog.listFiles()
        assert(filesBefore.length == 2)

        // Perform replaceWhere
        val newActions = Seq(
          AddAction(
            path = "date=2024-01-01/file3.split",
            partitionValues = Map("date" -> "2024-01-01"),
            size = 1500L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(150L)
          )
        )
        val version = transactionLog.replaceWhere(newActions, "date = '2024-01-01'")

        // Verify results
        assert(version > 0)
        val filesAfter = transactionLog.listFiles()
        assert(filesAfter.length == 2) // 1 new + 1 preserved

        // Verify correct files remain
        val date01Files = filesAfter.filter(_.partitionValues.get("date").contains("2024-01-01"))
        val date02Files = filesAfter.filter(_.partitionValues.get("date").contains("2024-01-02"))

        assert(date01Files.length == 1)
        assert(date01Files.head.path.contains("file3"))
        assert(date02Files.length == 1)
        assert(date02Files.head.path.contains("file2"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("TransactionLog.replaceWhere should throw for non-partitioned table") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        import io.indextables.spark.transaction.AddAction
        import org.apache.spark.sql.types._

        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("name", StringType)
        ))

        // Initialize WITHOUT partition columns
        transactionLog.initialize(schema)

        // Add a file
        transactionLog.addFile(AddAction(
          path = "file1.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        ))

        // Attempt replaceWhere should fail
        val exception = intercept[IllegalArgumentException] {
          transactionLog.replaceWhere(Seq.empty, "id = 1")
        }

        assert(exception.getMessage.contains("non-partitioned"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("TransactionLog.replaceWhere should throw for predicate referencing non-partition column") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        import io.indextables.spark.transaction.AddAction
        import org.apache.spark.sql.types._

        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("date", StringType)
        ))

        // Initialize with date as partition column
        transactionLog.initialize(schema, Seq("date"))

        // Add a file
        transactionLog.addFile(AddAction(
          path = "date=2024-01-01/file1.split",
          partitionValues = Map("date" -> "2024-01-01"),
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        ))

        // Attempt replaceWhere with non-partition column should fail
        val exception = intercept[IllegalArgumentException] {
          transactionLog.replaceWhere(Seq.empty, "name = 'test'")
        }

        assert(
          exception.getMessage.contains("non-partition") ||
            exception.getMessage.contains("Only partition columns")
        )
      } finally {
        transactionLog.close()
      }
    }
  }
}
