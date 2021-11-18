package org.elasticsearch.spark.sql

import org.apache.commons.logging.LogFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.elasticsearch.hadoop.rest.RestService

import scala.collection.immutable.HashMap
import scala.collection.mutable

case class ElasticsearchScan(schema: StructType, options: CaseInsensitiveStringMap, backingMap: mutable.Map[String, String],
                             isCountAgg: Boolean)
  extends Scan with Batch with SupportsReportStatistics with SupportsMetadata with Logging {
  options.forEach((key, value) => {
    backingMap.addOne(key, value)
  })
  val settings = new MapBackedSettings(backingMap)
  SchemaUtils.setRowInfo(settings, schema)

  override def readSchema(): StructType = {
    schema
  }

  override def planInputPartitions(): Array[InputPartition] = ???

  override def estimateStatistics(): Statistics = ???

  override def getMetaData(): Map[String, String] = {
    HashMap[String, String]()
  }

  override def toBatch = {
    new Batch() {
      override def planInputPartitions(): Array[InputPartition] = {
        val log = LogFactory.getLog(classOf[ElasticsearchScan])
        val settings = new MapBackedSettings(backingMap)
        if (isCountAgg) {
          val partitions = new Array[InputPartition](1)
          val partition = new ElasticsearchPartition(0, RestService.findPartitions(settings, log).get(0))
          partitions(0) = partition
          partitions
        } else {
          val rawPartitions = RestService.findPartitions(settings, log)
          val partitions = new Array[InputPartition](rawPartitions.size())
          var index = 0;
          rawPartitions.forEach(rawPartition => {
            val partition = new ElasticsearchPartition(index, rawPartition)
            partitions(index) = partition
            index = index + 1
          })
          partitions
        }
      }

      override def createReaderFactory(): PartitionReaderFactory = {
        ElasticsearchPartitionReaderFactory(backingMap, schema, isCountAgg)
      }
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = ???
}
