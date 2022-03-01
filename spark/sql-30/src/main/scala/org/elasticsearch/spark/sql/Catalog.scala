package org.elasticsearch.spark.sql

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.elasticsearch.hadoop.cfg.Settings
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider
import org.elasticsearch.hadoop.rest.{InitializationUtils, Resource, RestClient, RestRepository}
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser
import org.elasticsearch.spark.cfg.SparkSettingsManager

import java.io.InputStream
import java.util
import java.util.{Iterator, Properties, Map => JMap}
import scala.collection.mutable

class Catalog
  extends CatalogPlugin
    with TableCatalog
    with SupportsNamespaces {

  val Success = true

  @transient val backingMap = mutable.Map[String, String]()
  //  @transient var settings: Settings = null
  @transient var client: RestClient = null

  override def name(): String = ElasticsearchCatalog.NAME

  override def defaultNamespace(): Array[String] = {
    val ns = super.defaultNamespace()
    ns
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    options.forEach((key, value) => {
      backingMap.put(key, value)
    })
    val settings = new MapBackedSettings(backingMap)
    client = new RestClient(settings)
    val log = LogFactory.getLog(classOf[Catalog])
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaRowValueReader], log)
    InitializationUtils.setUserProviderIfNotSet(settings, classOf[HadoopUserProvider], log)
  }

  override def listNamespaces(): Array[Array[String]] = {
    Array.empty
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    Array(Array("org", "elasticsearch"))
  }

  override def loadNamespaceMetadata(namespace: Array[String]): JMap[String, String] = {
    import scala.collection.JavaConverters._
    Map.empty[String, String].asJava
  }

  override def createNamespace(namespace: Array[String], metadata: JMap[String, String]): Unit = {
    ???
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    ???
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    ???
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val result: util.Map[String, AnyRef] = client.get("*/_mapping", null)
    val indices = result.entrySet
    val indexNames = mutable.SortedSet[String]()
    indices.forEach(indexToMapping => indexNames.add(indexToMapping.getKey))

    var tables = new Array[Identifier](indexNames.size)
    indexNames.zipWithIndex.foreach{
      case(indexName, index) => tables(index) = new Identifier {
        override def namespace(): Array[String] = Array("org", "elasticsearch")

        override def name(): String = indexName
      }
    }
    tables
  }

  override def loadTable(ident: Identifier): Table = {
    new ElasticsearchTable(ident.name(), sparkSession = SparkSession.active, backingMap)
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: JMap[String, String]): Table = {
    ???
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    ???
  }

  override def dropTable(ident: Identifier): Boolean = {
    Success
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
  }

  override def toString = s"${this.getClass.getCanonicalName}($name)"
}

object ElasticsearchCatalog {
  val NAME = "es"
}