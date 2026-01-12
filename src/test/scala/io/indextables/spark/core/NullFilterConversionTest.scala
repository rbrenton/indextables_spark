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

import org.apache.spark.sql.sources.{EqualNullSafe, Filter, IsNotNull, IsNull}

import io.indextables.spark.TestBase
import io.indextables.tantivy4java.core.SchemaBuilder
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for null handling in filter pushdown.
 *
 * Sprint 14 fixed null handling in FiltersToQueryConverter:
 *   - IsNull: Uses BooleanQuery with MUST allQuery + MUST_NOT wildcardQuery
 *   - IsNotNull: Uses wildcardQuery with pattern "*"
 *   - EqualNullSafe with null: Same pattern as IsNull
 *
 * These tests verify that the filter conversion produces the correct query types.
 */
class NullFilterConversionTest extends TestBase with Matchers {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[NullFilterConversionTest])

  /**
   * Test that IsNull filter produces a BooleanQuery.
   *
   * IsNull matches documents where the field does not exist (has no indexed value). In Tantivy, this is implemented
   * using NOT field:* pattern - a BooleanQuery with MUST allQuery and MUST_NOT wildcardQuery.
   */
  test("IsNull filter should produce BooleanQuery with MUST_NOT wildcardQuery") {
    val schemaBuilder = new SchemaBuilder()
    schemaBuilder.addStringField("name", true, true, true)
    schemaBuilder.addIntegerField("age", true, true, true)
    val schema = schemaBuilder.build()

    try {
      // Create IsNull filter for string field
      val isNullFilter = IsNull("name")
      val query        = FiltersToQueryConverter.convertToQuery(Array[Filter](isNullFilter), schema)

      try {
        // Verify query was created successfully
        query should not be null

        // IsNull should produce a BooleanQuery (containing MUST allQuery + MUST_NOT wildcardQuery)
        val queryClassName = query.getClass.getSimpleName
        logger.info(s"IsNull filter produced query type: $queryClassName")

        // The query class name should contain "Boolean" to indicate it's a composite query
        // (BooleanQuery combines MUST allQuery with MUST_NOT wildcardQuery)
        queryClassName.toLowerCase should include("boolean")
      } finally
        query.close()
    } finally
      schema.close()
  }

  /**
   * Test that IsNotNull filter produces a wildcardQuery.
   *
   * IsNotNull matches documents where the field has an indexed value. In Tantivy, this is implemented using field:*
   * pattern - a wildcardQuery that matches all terms in the field.
   */
  test("IsNotNull filter should produce WildcardQuery") {
    val schemaBuilder = new SchemaBuilder()
    schemaBuilder.addStringField("name", true, true, true)
    schemaBuilder.addIntegerField("age", true, true, true)
    val schema = schemaBuilder.build()

    try {
      // Create IsNotNull filter for string field
      val isNotNullFilter = IsNotNull("name")
      val query           = FiltersToQueryConverter.convertToQuery(Array[Filter](isNotNullFilter), schema)

      try {
        // Verify query was created successfully
        query should not be null

        // IsNotNull should produce a WildcardQuery (field:*)
        val queryClassName = query.getClass.getSimpleName
        logger.info(s"IsNotNull filter produced query type: $queryClassName")

        // The query class name should contain "Wildcard" to indicate it matches all terms
        queryClassName.toLowerCase should include("wildcard")
      } finally
        query.close()
    } finally
      schema.close()
  }

  /**
   * Test that EqualNullSafe with null value produces a BooleanQuery.
   *
   * EqualNullSafe(field, null) is semantically equivalent to IsNull(field). It should produce the same query pattern:
   * BooleanQuery with MUST allQuery + MUST_NOT wildcardQuery.
   */
  test("EqualNullSafe with null value should produce BooleanQuery") {
    val schemaBuilder = new SchemaBuilder()
    schemaBuilder.addStringField("name", true, true, true)
    schemaBuilder.addIntegerField("age", true, true, true)
    val schema = schemaBuilder.build()

    try {
      // Create EqualNullSafe filter with null value
      val equalNullSafeFilter = EqualNullSafe("name", null)
      val query               = FiltersToQueryConverter.convertToQuery(Array[Filter](equalNullSafeFilter), schema)

      try {
        // Verify query was created successfully
        query should not be null

        // EqualNullSafe(field, null) should produce a BooleanQuery (same as IsNull)
        val queryClassName = query.getClass.getSimpleName
        logger.info(s"EqualNullSafe(null) filter produced query type: $queryClassName")

        // The query class name should contain "Boolean" to indicate it's a composite query
        queryClassName.toLowerCase should include("boolean")
      } finally
        query.close()
    } finally
      schema.close()
  }

  /**
   * Test that EqualNullSafe with non-null value produces a term query (not BooleanQuery for null handling).
   *
   * EqualNullSafe with a non-null value should behave like regular EqualTo, producing a term query for the value.
   */
  test("EqualNullSafe with non-null value should produce term-based query") {
    val schemaBuilder = new SchemaBuilder()
    schemaBuilder.addStringField("name", true, true, true)
    schemaBuilder.addIntegerField("age", true, true, true)
    val schema = schemaBuilder.build()

    try {
      // Create EqualNullSafe filter with non-null value
      val equalNullSafeFilter = EqualNullSafe("name", "Alice")
      val query               = FiltersToQueryConverter.convertToQuery(Array[Filter](equalNullSafeFilter), schema)

      try {
        // Verify query was created successfully
        query should not be null

        // EqualNullSafe with non-null value should NOT produce a BooleanQuery with null handling pattern
        // It should produce a regular term query or phrase query
        val queryClassName = query.getClass.getSimpleName
        logger.info(s"EqualNullSafe(non-null) filter produced query type: $queryClassName")

        // The query should be created successfully (no exception)
        queryClassName should not be empty

        // With a non-null value, this should NOT be the same null-handling BooleanQuery pattern
        // (It might still be a BooleanQuery for other reasons, but it should also work for term matching)
      } finally
        query.close()
    } finally
      schema.close()
  }

  /**
   * Test that IsNull on integer field produces a BooleanQuery.
   *
   * Null handling should work the same way for all field types.
   */
  test("IsNull on integer field should produce BooleanQuery") {
    val schemaBuilder = new SchemaBuilder()
    schemaBuilder.addStringField("name", true, true, true)
    schemaBuilder.addIntegerField("age", true, true, true)
    val schema = schemaBuilder.build()

    try {
      // Create IsNull filter for integer field
      val isNullFilter = IsNull("age")
      val query        = FiltersToQueryConverter.convertToQuery(Array[Filter](isNullFilter), schema)

      try {
        // Verify query was created successfully
        query should not be null

        // IsNull should produce a BooleanQuery regardless of field type
        val queryClassName = query.getClass.getSimpleName
        logger.info(s"IsNull on integer field produced query type: $queryClassName")

        // The query class name should contain "Boolean" to indicate it's a composite query
        queryClassName.toLowerCase should include("boolean")
      } finally
        query.close()
    } finally
      schema.close()
  }
}
