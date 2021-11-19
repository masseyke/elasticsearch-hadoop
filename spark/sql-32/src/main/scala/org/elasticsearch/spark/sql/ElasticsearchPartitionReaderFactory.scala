package org.elasticsearch.spark.sql

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types._
import org.elasticsearch.hadoop.rest._
import org.elasticsearch.hadoop.rest.query.MatchAllQueryBuilder
import org.elasticsearch.hadoop.rest.source.agg.CompositeAggQuery
import org.elasticsearch.hadoop.serialization.FieldType._
import org.elasticsearch.hadoop.serialization.builder.{JdkValueReader, ValueReader}
import org.elasticsearch.hadoop.serialization.dto.mapping.{Field, Mapping}
import org.elasticsearch.hadoop.serialization.{CompositeAggReader, FieldType}
import org.elasticsearch.hadoop.util.{ObjectUtils, StringUtils}
import org.elasticsearch.spark.sql._

import java.util
import java.util.Optional
import scala.collection.mutable

case class ElasticsearchPartitionReaderFactory(settingsMap: mutable.Map[String, String], schema: StructType,
                                               aggregations: util.Map[String, CompositeAggReader.AggInfo])
  extends PartitionReaderFactory {

  def createScrollReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    new PartitionReader[InternalRow] {
      val partitionDefinition: PartitionDefinition = inputPartition.asInstanceOf[ElasticsearchPartition].partitionDefinition
      val settings = new MapBackedSettings(settingsMap)
      val indexName = partitionDefinition.getIndex
      val shardId = partitionDefinition.getShardId
      val log = LogFactory.getLog(classOf[ElasticsearchPartitionReaderFactory])
      val fields : util.Collection[Field] = new util.ArrayList[Field]()
      val requiredColumns: Array[String] = new Array[String](schema.fields.size)
      schema.fields.zipWithIndex.foreach{case(field, index) => {
        val fieldName = field.name
        val fieldType = field.dataType match {
          case NullType => NULL
          case BinaryType => BINARY
          case BooleanType => BOOLEAN
          case ByteType => BYTE
          case ShortType => SHORT
          case IntegerType => INTEGER
          case LongType => LONG
          case FloatType => FLOAT
          case DoubleType => DOUBLE
          case StringType => STRING
          case TimestampType => DATE
          case _ => OBJECT
        }
        fields.add(new Field(fieldName, fieldType))
        requiredColumns(index) = fieldName
      }}
      settingsMap.put(Utils.DATA_SOURCE_REQUIRED_COLUMNS, StringUtils.concatenate(requiredColumns.asInstanceOf[Array[Object]], StringUtils.DEFAULT_DELIMITER))
      //      settingsMap.put(ConfigurationOptions.ES_READ_METADATA, "true");
      val partitionReader = RestService.createReader(
        settings,
        PartitionDefinition.builder(settings, new Mapping(indexName, indexName, fields)).build(indexName, shardId),
        log
      )
      val scrollQuery = partitionReader.scrollQuery()

      override def next(): Boolean = {
        scrollQuery.hasNext
      }

      override def get(): InternalRow = {
        val row = scrollQuery.next()
        val realRow: ScalaEsRow = row(1).asInstanceOf[ScalaEsRow]
        new ElasticsearchRow(realRow)
      }

      override def close(): Unit = {
        scrollQuery.close()
      }
    }
  }

  def createAggReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    val settings = new MapBackedSettings(settingsMap)
    val restRepository = new RestRepository(settings)
    val aggs = new util.HashMap[String, CompositeAggReader.AggInfo]
    System.out.println("About to create pit")
    val pit = restRepository.createPointInTime()
    System.out.println("Pit created")
    val valueReader: ValueReader = ObjectUtils.instantiate("org.elasticsearch.spark.sql.ScalaRowValueReader", settings)

    val mappings = getMappingFromSchema()
    val aggList = new util.ArrayList[CompositeAggReader.AggInfo](aggs.values)
    System.out.println("About to create aggQuery")
    val aggQuery = new CompositeAggQuery[AnyRef](
      restRepository,
      "_search",
      pit,
      MatchAllQueryBuilder.MATCH_ALL,
      util.Arrays.asList("message.keyword"),
      aggList,
      new CompositeAggReader[AnyRef](valueReader, null, mappings, Optional.empty, aggs))
    System.out.println("About to create partition reader")
    new PartitionReader[InternalRow] {
      override def next(): Boolean = {
        System.out.println("About to check for next")
        aggQuery.hasNext
      }

      override def get(): InternalRow = {
        System.out.println("Getting next")
        new ElasticsearchRow(aggQuery.next().asInstanceOf[ScalaEsRow])
      }

      override def close(): Unit = aggQuery.close()
    }
  }

  def getMappingFromSchema(): util.HashMap[String, FieldType] = {
    val mappings = new util.HashMap[String, FieldType]
    mappings.put("dept", FieldType.KEYWORD)
    mappings.put("age", FieldType.INTEGER)
    mappings.put("salary", FieldType.INTEGER)
    mappings
  }

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    if (aggregations.isEmpty) {
      createScrollReader(inputPartition)
    } else {
      createAggReader(inputPartition)
    }
  }

}
