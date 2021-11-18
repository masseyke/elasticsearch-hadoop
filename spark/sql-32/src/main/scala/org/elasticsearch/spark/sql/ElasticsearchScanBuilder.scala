package org.elasticsearch.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.elasticsearch.hadoop.cfg.Settings

import scala.collection.mutable

case class ElasticsearchScanBuilder(
                                     sparkSession: SparkSession,
                                     schema: StructType,
                                     options: CaseInsensitiveStringMap,
                                     backingMap: mutable.Map[String, String])
  extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters with SupportsPushDownAggregates {
  var updatedSchema: StructType = schema

  private val _pushedFilters: Array[Filter] = Array.empty

  private var isCountAgg = false

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    ElasticsearchScan(updatedSchema, options, backingMap, isCountAgg)
  }

  override def pruneColumns(structType: StructType): Unit = {
    println("Need to prune everything in " + structType.fields)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    println("In pushAggregation...")
    val groupByColumns = aggregation.groupByColumns();
    groupByColumns.foreach(col => {
      println("group by column: ")
      col.fieldNames().foreach(fieldName => println("\tfield: " + fieldName))
    } )
    val aggregationExpressions = aggregation.aggregateExpressions()
    println("Aggregate functions: ")
    aggregationExpressions.foreach(aggregateFunc => {
      println("\tdescribe: " + aggregateFunc.describe())
      println("\tclass: " + aggregateFunc.getClass)
      println("\t" + aggregateFunc)
      val hasCountAgg = aggregateFunc.isInstanceOf[CountStar] || aggregateFunc.isInstanceOf[Count]
      if (hasCountAgg) {
        isCountAgg = true
        val columnName = "count(\"*\")"
        val fields = new Array[StructField](1)
        val metadata = new MetadataBuilder()
        val nullable = false
        val columnType = IntegerType
        fields(0) = StructField(columnName, columnType, nullable, metadata.build())
        updatedSchema = StructType(fields)
        return true
      }
    })
    return false
  }
}
