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
  BinaryComparison,
  BoundReference,
  Cast,
  Expression,
  In,
  Literal
}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

import org.slf4j.LoggerFactory

/**
 * Utilities for parsing, validating, and evaluating partition predicates. This class extracts common logic used by
 * commands that need to filter partitions based on WHERE clauses (e.g., MERGE SPLITS, DROP PARTITIONS).
 */
object PartitionPredicateUtils {

  private val logger = LoggerFactory.getLogger(PartitionPredicateUtils.getClass)

  /**
   * Parse partition predicates from raw WHERE clause text and validate that they only reference partition columns.
   *
   * @param predicates
   *   Raw predicate strings from the WHERE clause
   * @param partitionSchema
   *   Schema defining the partition columns
   * @param sparkSession
   *   SparkSession for parsing expressions
   * @return
   *   Sequence of parsed and validated Expression objects
   * @throws IllegalArgumentException
   *   if predicates reference non-partition columns or cannot be parsed
   */
  def parseAndValidatePredicates(
    predicates: Seq[String],
    partitionSchema: StructType,
    sparkSession: SparkSession
  ): Seq[Expression] = {

    // If no partition columns are defined, reject any WHERE clauses
    if (partitionSchema.isEmpty && predicates.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause not supported for non-partitioned tables. Partition predicates: ${predicates.mkString(", ")}"
      )
    }

    predicates.flatMap { predicate =>
      try {
        val expression = sparkSession.sessionState.sqlParser.parseExpression(predicate)
        validatePartitionColumnReferences(expression, partitionSchema)
        Some(expression)
      } catch {
        case ex: IllegalArgumentException =>
          // Re-throw validation errors as-is
          throw ex
        case ex: Exception =>
          logger.error(s"Failed to parse partition predicate: $predicate", ex)
          throw new IllegalArgumentException(s"Invalid partition predicate: $predicate", ex)
      }
    }
  }

  /**
   * Validate that the expression only references partition columns.
   *
   * @param expression
   *   The expression to validate
   * @param partitionSchema
   *   Schema defining the valid partition columns
   * @throws IllegalArgumentException
   *   if the expression references non-partition columns
   */
  def validatePartitionColumnReferences(expression: Expression, partitionSchema: StructType): Unit = {
    val partitionColumns  = partitionSchema.fieldNames.toSet
    val referencedColumns = expression.references.map(_.name).toSet

    // Check if any referenced columns are not partition columns
    val invalidColumns = referencedColumns -- partitionColumns
    if (invalidColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause references non-partition columns: ${invalidColumns.mkString(", ")}. " +
          s"Only partition columns are allowed: ${partitionColumns.mkString(", ")}"
      )
    }

    // Check if WHERE clause is empty (no column references)
    if (referencedColumns.isEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause must reference at least one partition column. " +
          s"Available partition columns: ${partitionColumns.mkString(", ")}"
      )
    }
  }

  /**
   * Create an InternalRow from partition values for predicate evaluation.
   *
   * @param partitionValues
   *   Map of partition column names to values
   * @param partitionSchema
   *   Schema defining the partition columns
   * @return
   *   InternalRow with partition values
   */
  def createRowFromPartitionValues(
    partitionValues: Map[String, String],
    partitionSchema: StructType
  ): InternalRow = {
    val values = partitionSchema.fieldNames.map { fieldName =>
      partitionValues.get(fieldName) match {
        case Some(value) => UTF8String.fromString(value)
        case None        => null
      }
    }
    InternalRow.fromSeq(values)
  }

  /**
   * Resolve an expression against a schema to handle UnresolvedAttribute references. For numeric comparisons, casts
   * string partition values to the literal's type for proper numeric (not lexicographic) comparison.
   */
  def resolveExpression(expression: Expression, schema: StructType): Expression = {
    val columnTypeHints = inferColumnTypesFromExpression(expression)

    expression.transform {
      case unresolvedAttr: UnresolvedAttribute =>
        val fieldName  = unresolvedAttr.name
        val fieldIndex = schema.fieldIndex(fieldName)
        val field      = schema(fieldIndex)
        val boundRef   = BoundReference(fieldIndex, field.dataType, field.nullable)

        columnTypeHints.get(fieldName) match {
          case Some(targetType) if targetType != StringType => Cast(boundRef, targetType)
          case _                                            => boundRef
        }

      case literal: Literal => literal
    }
  }

  /** Infer target types for columns based on literals used in comparisons. */
  private def inferColumnTypesFromExpression(expression: Expression): Map[String, DataType] = {
    val typeHints = scala.collection.mutable.Map[String, DataType]()

    def extractFromOperands(left: Expression, right: Expression): Unit =
      (left, right) match {
        case (attr: UnresolvedAttribute, lit: Literal) if isNumericOrDateType(lit.dataType) =>
          typeHints(attr.name) = lit.dataType
        case (lit: Literal, attr: UnresolvedAttribute) if isNumericOrDateType(lit.dataType) =>
          typeHints(attr.name) = lit.dataType
        case _ =>
      }

    expression.foreach {
      case cmp: BinaryComparison =>
        extractFromOperands(cmp.left, cmp.right)
      case In(attr: UnresolvedAttribute, values) =>
        values.collectFirst {
          case lit: Literal if isNumericOrDateType(lit.dataType) => lit.dataType
        }.foreach { dt => typeHints(attr.name) = dt }
      case _ =>
    }

    typeHints.toMap
  }

  private def isNumericOrDateType(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | ShortType | ByteType => true
    case FloatType | DoubleType                        => true
    case _: DecimalType                                => true
    case DateType | TimestampType                      => true
    case _                                             => false
  }

  /**
   * Evaluate whether a partition matches the given predicates.
   *
   * @param partitionValues
   *   The partition values to evaluate
   * @param partitionSchema
   *   Schema defining the partition columns
   * @param predicates
   *   Parsed predicate expressions
   * @return
   *   true if the partition matches all predicates
   */
  def evaluatePredicates(
    partitionValues: Map[String, String],
    partitionSchema: StructType,
    predicates: Seq[Expression]
  ): Boolean = {
    if (predicates.isEmpty) return true

    val row = createRowFromPartitionValues(partitionValues, partitionSchema)
    predicates.forall { predicate =>
      try {
        val resolvedPredicate = resolveExpression(predicate, partitionSchema)
        resolvedPredicate.eval(row).asInstanceOf[Boolean]
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to evaluate predicate against partition $partitionValues: ${ex.getMessage}")
          false // Conservative: exclude partition if evaluation fails
      }
    }
  }

  /**
   * Filter AddActions by partition predicates.
   *
   * @param addActions
   *   The AddActions to filter
   * @param partitionSchema
   *   Schema defining the partition columns
   * @param predicates
   *   Parsed predicate expressions
   * @return
   *   AddActions that match the predicates
   */
  def filterAddActionsByPredicates(
    addActions: Seq[AddAction],
    partitionSchema: StructType,
    predicates: Seq[Expression]
  ): Seq[AddAction] = {
    if (predicates.isEmpty) return addActions

    val filtered = addActions.filter { action =>
      evaluatePredicates(action.partitionValues, partitionSchema, predicates)
    }

    val prunedCount = addActions.length - filtered.length
    if (prunedCount > 0) {
      logger.info(s"Partition predicate filtering: matched ${filtered.length} of ${addActions.length} splits")
    }

    filtered
  }

  /**
   * Build a partition schema from partition column names. All partition columns are treated as StringType for
   * evaluation purposes since partition values are stored as strings in the transaction log.
   *
   * @param partitionColumns
   *   Sequence of partition column names
   * @return
   *   StructType with all columns as StringType
   */
  def buildPartitionSchema(partitionColumns: Seq[String]): StructType =
    StructType(partitionColumns.map(name => StructField(name, StringType, nullable = true)))
}
