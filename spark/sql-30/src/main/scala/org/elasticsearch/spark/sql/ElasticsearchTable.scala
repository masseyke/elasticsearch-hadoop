package org.elasticsearch.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.elasticsearch.hadoop.cfg.{ConfigurationOptions, Settings}

import java.util
import scala.collection.mutable

case class ElasticsearchTable(
                               name: String,
                               sparkSession: SparkSession,
                               backingMap: mutable.Map[String, String])
  extends Table with SupportsRead with SupportsWrite {
  backingMap.put(ConfigurationOptions.ES_RESOURCE_READ, name)
  backingMap.put(ConfigurationOptions.ES_RESOURCE_WRITE, name)
  backingMap.put(ConfigurationOptions.ES_RESOURCE, name)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ElasticsearchScanBuilder = {
    SchemaUtils.setRowInfo(new MapBackedSettings(backingMap), schema)
    ElasticsearchScanBuilder(sparkSession, schema, options, backingMap)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = ElasticsearchWrite()
    }

  override def schema(): StructType = {
    SchemaUtils.discoverMapping(new MapBackedSettings(backingMap)).struct
  }

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = new util.HashSet[TableCapability]
    capabilities.add(TableCapability.BATCH_READ)
    capabilities.add(TableCapability.BATCH_WRITE)
    capabilities
  }
}