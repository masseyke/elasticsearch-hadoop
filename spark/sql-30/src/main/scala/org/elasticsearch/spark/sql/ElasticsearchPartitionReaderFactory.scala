package org.elasticsearch.spark.sql

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types._
import org.elasticsearch.hadoop.rest._
import org.elasticsearch.hadoop.rest.query.MatchAllQueryBuilder
import org.elasticsearch.hadoop.rest.source.agg.CompositeAggQuery
import org.elasticsearch.hadoop.serialization.CompositeAggReader.AggInfo
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
                                               groupBys: util.List[String], aggregations: util.Map[String, CompositeAggReader.AggInfo])
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

  def createCompositeAggReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    val settings = new MapBackedSettings(settingsMap)
    val restRepository = new RestRepository(settings)
    val pit = restRepository.createPointInTime()
    val valueReader: ValueReader = ObjectUtils.instantiate("org.elasticsearch.spark.sql.ScalaRowValueReader", settings)

    val mappings = getMappingFromSchema()
    val aggList = new util.ArrayList[CompositeAggReader.AggInfo](aggregations.values)
    val aggQuery = new CompositeAggQuery[AnyRef](
      restRepository,
      "_search",
      pit,
      MatchAllQueryBuilder.MATCH_ALL,
      groupBys,
      aggList,
      new CompositeAggReader[AnyRef](valueReader, null, mappings, Optional.empty.asInstanceOf[Optional[AggInfo]], aggregations))
    new PartitionReader[InternalRow] {
      override def next(): Boolean = {
        aggQuery.hasNext
      }

      override def get(): InternalRow = {
        new ElasticsearchRow(aggQuery.next().asInstanceOf[ScalaEsRow])
      }

      override def close(): Unit = aggQuery.close()
    }
  }

  def getMappingFromSchema(): util.HashMap[String, FieldType] = {
    val mappings = new util.HashMap[String, FieldType]
    schema.foreach(field => mappings.put(field.name, lookupEsTypeFromSparkType(field.dataType)))
    mappings
  }

  def lookupEsTypeFromSparkType(sparkType: DataType): FieldType = {
    sparkType match {
      case StringType => FieldType.KEYWORD
      case IntegerType => FieldType.INTEGER
      case LongType => FieldType.LONG
    }
  }

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    if (aggregations.isEmpty && groupBys.isEmpty) {
      createScrollReader(inputPartition)
    } else {
      createCompositeAggReader(inputPartition)
    }
  }

}