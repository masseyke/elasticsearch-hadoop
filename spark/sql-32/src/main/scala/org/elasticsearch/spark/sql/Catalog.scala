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
    println(s"defaultNamespace = ${ns.toSeq}")
    ns
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    println("Options")
    options.forEach((key, value) => {
      println("\t" + key + ": " + value)
      backingMap.addOne(key, value)
    })
    val settings = new MapBackedSettings(backingMap)
    client = new RestClient(settings)
    val log = LogFactory.getLog(classOf[Catalog])
    InitializationUtils.setValueReaderIfNotSet(settings, classOf[ScalaRowValueReader], log)
    InitializationUtils.setUserProviderIfNotSet(settings, classOf[HadoopUserProvider], log)
    import scala.collection.JavaConverters._
    println(s">>> initialize($name, ${options.asScala})")
  }

  override def listNamespaces(): Array[Array[String]] = {
    println(">>> listNamespaces()")
    Array.empty
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    println(s">>> listNamespaces($namespace)")
    Array(Array("org", "elasticsearch"))
  }

  override def loadNamespaceMetadata(namespace: Array[String]): JMap[String, String] = {
    println(s">>> loadNamespaceMetadata(${namespace.toSeq})")
    import scala.collection.JavaConverters._
    Map.empty[String, String].asJava
  }

  override def createNamespace(namespace: Array[String], metadata: JMap[String, String]): Unit = {
    import scala.collection.JavaConverters._
    println(s">>> createNamespace($namespace, ${metadata.asScala})")
    ???
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    println(s">>> alterNamespace($namespace, $changes)")
    ???
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    println(s">>> dropNamespace($namespace)")
    ???
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    println(s">>> listTables(${namespace.toSeq})")
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
    println(s">>> loadTable($ident)")
    new ElasticsearchTable(ident.name(), sparkSession = SparkSession.active, backingMap)
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: JMap[String, String]): Table = {
    import scala.collection.JavaConverters._
    println(s">>> createTable($ident, $schema, $partitions, ${properties.asScala})")
    ???
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    println(s">>> alterTable($ident, $changes)")
    ???
  }

  override def dropTable(ident: Identifier): Boolean = {
    println(s">>> dropTable($ident)")
    Success
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    println(s">>> renameTable($oldIdent, $newIdent)")
  }

  override def toString = s"${this.getClass.getCanonicalName}($name)"
}

object ElasticsearchCatalog {
  val NAME = "es"
}