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

import org.apache.spark.sql.catalyst.expressions.{
  And,
  BoundReference,
  Cast,
  EqualTo,
  GreaterThan,
  GreaterThanOrEqual,
  In,
  LessThan,
  LessThanOrEqual,
  Literal,
  Not
}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PartitionPredicateUtilsTest extends AnyFunSuite with Matchers {

  // BETWEEN is transformed by Spark parser into: And(GreaterThanOrEqual, LessThanOrEqual)
  test("BETWEEN with integer literals should use numeric comparison") {
    val schema = StructType(Seq(StructField("month", StringType, nullable = true)))

    // Simulates: month BETWEEN 2 AND 6
    val betweenExpr = And(
      GreaterThanOrEqual(UnresolvedAttribute("month"), Literal(2)),
      LessThanOrEqual(UnresolvedAttribute("month"), Literal(6))
    )

    // Month 10 should NOT match (lexicographically "10" < "2", but numerically 10 > 6)
    val month10 = Map("month" -> "10")
    PartitionPredicateUtils.evaluatePredicates(month10, schema, Seq(betweenExpr)) shouldBe false

    // Month 3 should match
    val month3 = Map("month" -> "3")
    PartitionPredicateUtils.evaluatePredicates(month3, schema, Seq(betweenExpr)) shouldBe true

    // Month 1 should NOT match
    val month1 = Map("month" -> "1")
    PartitionPredicateUtils.evaluatePredicates(month1, schema, Seq(betweenExpr)) shouldBe false
  }

  test("BETWEEN with double literals should use numeric comparison") {
    val schema = StructType(Seq(StructField("price", StringType, nullable = true)))

    // Simulates: price BETWEEN 10.5 AND 20.5
    val betweenExpr = And(
      GreaterThanOrEqual(UnresolvedAttribute("price"), Literal(10.5)),
      LessThanOrEqual(UnresolvedAttribute("price"), Literal(20.5))
    )

    val price15 = Map("price" -> "15.0")
    PartitionPredicateUtils.evaluatePredicates(price15, schema, Seq(betweenExpr)) shouldBe true

    val price5 = Map("price" -> "5.0")
    PartitionPredicateUtils.evaluatePredicates(price5, schema, Seq(betweenExpr)) shouldBe false

    val price25 = Map("price" -> "25.0")
    PartitionPredicateUtils.evaluatePredicates(price25, schema, Seq(betweenExpr)) shouldBe false
  }

  test("IN clause with integer literals should use numeric comparison") {
    val schema = StructType(Seq(StructField("month", StringType, nullable = true)))

    // Simulates: month IN (1, 2, 10, 11)
    val inExpr = In(
      UnresolvedAttribute("month"),
      Seq(Literal(1), Literal(2), Literal(10), Literal(11))
    )

    val month10 = Map("month" -> "10")
    PartitionPredicateUtils.evaluatePredicates(month10, schema, Seq(inExpr)) shouldBe true

    val month5 = Map("month" -> "5")
    PartitionPredicateUtils.evaluatePredicates(month5, schema, Seq(inExpr)) shouldBe false
  }

  test("comparison with negative numbers should work correctly") {
    val schema = StructType(Seq(StructField("temp", StringType, nullable = true)))

    // Simulates: temp > -10
    val gtExpr = GreaterThan(UnresolvedAttribute("temp"), Literal(-10))

    val temp5 = Map("temp" -> "5")
    PartitionPredicateUtils.evaluatePredicates(temp5, schema, Seq(gtExpr)) shouldBe true

    val tempNeg20 = Map("temp" -> "-20")
    PartitionPredicateUtils.evaluatePredicates(tempNeg20, schema, Seq(gtExpr)) shouldBe false

    val tempNeg5 = Map("temp" -> "-5")
    PartitionPredicateUtils.evaluatePredicates(tempNeg5, schema, Seq(gtExpr)) shouldBe true
  }

  test("boundary values should be handled correctly") {
    val schema = StructType(Seq(StructField("value", StringType, nullable = true)))

    // Simulates: value >= 10 AND value <= 10 (exact match)
    val exactMatch = And(
      GreaterThanOrEqual(UnresolvedAttribute("value"), Literal(10)),
      LessThanOrEqual(UnresolvedAttribute("value"), Literal(10))
    )

    val value10 = Map("value" -> "10")
    PartitionPredicateUtils.evaluatePredicates(value10, schema, Seq(exactMatch)) shouldBe true

    val value9 = Map("value" -> "9")
    PartitionPredicateUtils.evaluatePredicates(value9, schema, Seq(exactMatch)) shouldBe false

    val value11 = Map("value" -> "11")
    PartitionPredicateUtils.evaluatePredicates(value11, schema, Seq(exactMatch)) shouldBe false
  }

  test("NULL result from cast should return false (not throw NullPointerException)") {
    val schema = StructType(Seq(StructField("value", StringType, nullable = true)))

    // Simulates: value > 10 with invalid numeric string
    val gtExpr = GreaterThan(UnresolvedAttribute("value"), Literal(10))

    // "abc" cannot be cast to integer - should return false, not throw
    val invalidValue = Map("value" -> "abc")
    PartitionPredicateUtils.evaluatePredicates(invalidValue, schema, Seq(gtExpr)) shouldBe false
  }

  test("missing partition value should return false") {
    val schema = StructType(Seq(StructField("month", StringType, nullable = true)))

    val gtExpr = GreaterThan(UnresolvedAttribute("month"), Literal(5))

    // Empty partition values
    val emptyPartition = Map.empty[String, String]
    PartitionPredicateUtils.evaluatePredicates(emptyPartition, schema, Seq(gtExpr)) shouldBe false
  }

  test("empty predicates should match all partitions") {
    val schema     = StructType(Seq(StructField("month", StringType, nullable = true)))
    val anyValue   = Map("month" -> "whatever")
    val predicates = Seq.empty[org.apache.spark.sql.catalyst.expressions.Expression]

    PartitionPredicateUtils.evaluatePredicates(anyValue, schema, predicates) shouldBe true
  }

  test("string equality should work without type inference") {
    val schema = StructType(Seq(StructField("status", StringType, nullable = true)))

    // String literal - no type inference needed
    val eqExpr = EqualTo(UnresolvedAttribute("status"), Literal("active"))

    val activePartition = Map("status" -> "active")
    PartitionPredicateUtils.evaluatePredicates(activePartition, schema, Seq(eqExpr)) shouldBe true

    val inactivePartition = Map("status" -> "inactive")
    PartitionPredicateUtils.evaluatePredicates(inactivePartition, schema, Seq(eqExpr)) shouldBe false
  }

  test("resolveExpression should cast columns when compared to numeric literals") {
    val schema = StructType(Seq(StructField("month", StringType, nullable = true)))

    val gtExpr   = GreaterThan(UnresolvedAttribute("month"), Literal(5))
    val resolved = PartitionPredicateUtils.resolveExpression(gtExpr, schema)

    // The left side should be wrapped in Cast
    resolved match {
      case GreaterThan(Cast(BoundReference(0, StringType, _), IntegerType, _, _), Literal(5, IntegerType)) =>
        succeed
      case other =>
        fail(s"Expected Cast wrapper, got: $other")
    }
  }

  test("resolveExpression should not cast columns when compared to string literals") {
    val schema = StructType(Seq(StructField("status", StringType, nullable = true)))

    val eqExpr   = EqualTo(UnresolvedAttribute("status"), Literal("active"))
    val resolved = PartitionPredicateUtils.resolveExpression(eqExpr, schema)

    // The left side should NOT be wrapped in Cast
    resolved match {
      case EqualTo(BoundReference(0, StringType, _), Literal(_, StringType)) =>
        succeed
      case other =>
        fail(s"Expected no Cast wrapper for string comparison, got: $other")
    }
  }

  test("createRowFromPartitionValues should handle missing columns as null") {
    val schema = StructType(
      Seq(
        StructField("col1", StringType, nullable = true),
        StructField("col2", StringType, nullable = true)
      )
    )

    val partialValues = Map("col1" -> "value1")
    val row           = PartitionPredicateUtils.createRowFromPartitionValues(partialValues, schema)

    row.isNullAt(0) shouldBe false
    row.isNullAt(1) shouldBe true
  }

  test("buildPartitionSchema should create StringType for all columns") {
    val columns = Seq("year", "month", "day")
    val schema  = PartitionPredicateUtils.buildPartitionSchema(columns)

    schema.length shouldBe 3
    schema.fields.foreach { field =>
      field.dataType shouldBe StringType
      field.nullable shouldBe true
    }
    schema.fieldNames shouldBe columns
  }

  test("filterAddActionsByPredicates should filter based on predicates") {
    val schema = StructType(Seq(StructField("month", StringType, nullable = true)))

    val actions = Seq(
      AddAction("path1", Map("month" -> "1"), 100L, 0L, dataChange = true),
      AddAction("path2", Map("month" -> "5"), 100L, 0L, dataChange = true),
      AddAction("path3", Map("month" -> "10"), 100L, 0L, dataChange = true)
    )

    // Simulates: month BETWEEN 2 AND 6
    val betweenExpr = And(
      GreaterThanOrEqual(UnresolvedAttribute("month"), Literal(2)),
      LessThanOrEqual(UnresolvedAttribute("month"), Literal(6))
    )

    val filtered = PartitionPredicateUtils.filterAddActionsByPredicates(actions, schema, Seq(betweenExpr))

    filtered should have length 1
    filtered.head.partitionValues("month") shouldBe "5"
  }

  test("filterAddActionsByPredicates with empty predicates should return all actions") {
    val schema = StructType(Seq(StructField("month", StringType, nullable = true)))

    val actions = Seq(
      AddAction("path1", Map("month" -> "1"), 100L, 0L, dataChange = true),
      AddAction("path2", Map("month" -> "5"), 100L, 0L, dataChange = true)
    )

    val filtered = PartitionPredicateUtils.filterAddActionsByPredicates(actions, schema, Seq.empty)

    filtered should have length 2
  }

  test("long type comparison should work correctly") {
    val schema = StructType(Seq(StructField("timestamp", StringType, nullable = true)))

    // Large long value
    val gtExpr = GreaterThan(UnresolvedAttribute("timestamp"), Literal(1000000000000L))

    val largeTs = Map("timestamp" -> "1500000000000")
    PartitionPredicateUtils.evaluatePredicates(largeTs, schema, Seq(gtExpr)) shouldBe true

    val smallTs = Map("timestamp" -> "500000000000")
    PartitionPredicateUtils.evaluatePredicates(smallTs, schema, Seq(gtExpr)) shouldBe false
  }

  test("decimal type comparison should work correctly") {
    val schema = StructType(Seq(StructField("amount", StringType, nullable = true)))

    val decimalLit = Literal(new java.math.BigDecimal("100.50"))
    val gtExpr     = GreaterThan(UnresolvedAttribute("amount"), decimalLit)

    val largeAmount = Map("amount" -> "150.25")
    PartitionPredicateUtils.evaluatePredicates(largeAmount, schema, Seq(gtExpr)) shouldBe true

    val smallAmount = Map("amount" -> "50.25")
    PartitionPredicateUtils.evaluatePredicates(smallAmount, schema, Seq(gtExpr)) shouldBe false
  }

  test("mixed int and double literals should promote to double") {
    val schema = StructType(Seq(StructField("value", StringType, nullable = true)))

    // Simulates: value > 2 AND value < 6.5 (int and double mixed)
    val mixedExpr = And(
      GreaterThan(UnresolvedAttribute("value"), Literal(2)),      // IntegerType
      LessThan(UnresolvedAttribute("value"), Literal(6.5))        // DoubleType
    )

    // "5" should match (2 < 5.0 < 6.5)
    val value5 = Map("value" -> "5")
    PartitionPredicateUtils.evaluatePredicates(value5, schema, Seq(mixedExpr)) shouldBe true

    // "2" should NOT match (not > 2)
    val value2 = Map("value" -> "2")
    PartitionPredicateUtils.evaluatePredicates(value2, schema, Seq(mixedExpr)) shouldBe false

    // "6.5" should NOT match (not < 6.5)
    val value65 = Map("value" -> "6.5")
    PartitionPredicateUtils.evaluatePredicates(value65, schema, Seq(mixedExpr)) shouldBe false
  }

  test("mixed int and long literals should promote to long") {
    val schema = StructType(Seq(StructField("id", StringType, nullable = true)))

    // Simulates: id >= 100 AND id <= 1000000000000L (int and long mixed)
    val mixedExpr = And(
      GreaterThanOrEqual(UnresolvedAttribute("id"), Literal(100)),           // IntegerType
      LessThanOrEqual(UnresolvedAttribute("id"), Literal(1000000000000L))    // LongType
    )

    val validId = Map("id" -> "500000000000")
    PartitionPredicateUtils.evaluatePredicates(validId, schema, Seq(mixedExpr)) shouldBe true

    val smallId = Map("id" -> "50")
    PartitionPredicateUtils.evaluatePredicates(smallId, schema, Seq(mixedExpr)) shouldBe false
  }

  test("type promotion should handle same column in multiple comparisons") {
    val schema = StructType(Seq(StructField("score", StringType, nullable = true)))

    // Simulates: score > 10 AND score < 20 AND score != 15.5 (multiple comparisons)
    val multiExpr = And(
      And(
        GreaterThan(UnresolvedAttribute("score"), Literal(10)),    // IntegerType
        LessThan(UnresolvedAttribute("score"), Literal(20))        // IntegerType
      ),
      Not(EqualTo(UnresolvedAttribute("score"), Literal(15.5)))    // DoubleType - should promote
    )

    val score12 = Map("score" -> "12")
    PartitionPredicateUtils.evaluatePredicates(score12, schema, Seq(multiExpr)) shouldBe true

    val score155 = Map("score" -> "15.5")
    PartitionPredicateUtils.evaluatePredicates(score155, schema, Seq(multiExpr)) shouldBe false
  }
}
