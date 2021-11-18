package org.elasticsearch.spark.sql

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, Decimal, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.elasticsearch.hadoop.cfg.{ConfigurationOptions, Settings}
import org.elasticsearch.hadoop.rest._
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.FieldType.{BINARY, BOOLEAN, BYTE, DATE, DOUBLE, FLOAT, HALF_FLOAT, INTEGER, KEYWORD, LONG, NESTED, NULL, OBJECT, SCALED_FLOAT, SHORT, STRING, TEXT}
import org.elasticsearch.hadoop.serialization.dto.mapping.{Field, Mapping}
import org.elasticsearch.hadoop.util.StringUtils

import java.util
import scala.collection.mutable

case class ElasticsearchPartitionReaderFactory(settingsMap: mutable.Map[String, String], schema: StructType, isCountAgg: Boolean) extends
  PartitionReaderFactory {

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
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
      partitionReader.client.count()
      val scrollQuery = partitionReader.scrollQuery()
      override def next(): Boolean = {
        System.out.println("hasNext row: " + scrollQuery.hasNext)
        scrollQuery.hasNext
      }

      override def get(): InternalRow = {
        val row = scrollQuery.next()
        val realRow: ScalaEsRow = row(1).asInstanceOf[ScalaEsRow]
        System.out.println("next row")
        new ElasticsearchRow(realRow)
      }

      override def close(): Unit = {
          scrollQuery.close()
      }
    }
  }

}
