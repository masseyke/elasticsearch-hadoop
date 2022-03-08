package org.elasticsearch.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{And, BaseRelation, CreatableRelationProvider, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, InsertableRelation, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or}
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.elasticsearch.hadoop.cfg.{InternalConfigurationOptions, Settings}
import org.elasticsearch.hadoop.serialization.{CompositeAggReader, FieldType}
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator
import org.elasticsearch.hadoop.util.{FastByteArrayOutputStream, IOUtils, StringUtils}
import org.elasticsearch.hadoop.util.SettingsUtils.isEs50
import org.elasticsearch.spark.serialization.ScalaValueWriter

import java.util
import java.util.{Calendar, Date, Locale}
import javax.xml.bind.DatatypeConverter
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{LinkedHashSet, ListBuffer}

case class ElasticsearchScanBuilder(
                                     sparkSession: SparkSession,
                                     schema: StructType,
                                     options: CaseInsensitiveStringMap,
                                     backingMap: mutable.Map[String, String])
  extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters with SupportsPushDownAggregates {
  var updatedSchema: StructType = schema
  @transient lazy val valueWriter = { new ScalaValueWriter }

  private var _pushedFilters: Array[Filter] = Array.empty
  private var filterStrings: Array[String] = Array.empty
  val aggregations: util.Map[String, CompositeAggReader.AggInfo] = new util.HashMap
  val groupBys: util.List[String] = new util.ArrayList[String]()
  val filters: util.List[String] = new util.ArrayList[String]()

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    backingMap.remove(InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS)
    ElasticsearchScan(updatedSchema, options, backingMap, filterStrings, groupBys,
      aggregations)
  }

  override def pruneColumns(structType: StructType): Unit = {
    updatedSchema = new StructType(schema.fields.filter(field => structType.fields.contains(field)))
  }

  private def createDSLFromFilters(filters: Array[Filter], strictPushDown: Boolean, isES50: Boolean): (Array[Filter], Array[String]) = {
    val unusedFilters = new ListBuffer[Filter]()
    val queries = filters.map(filter => (filter, translateFilter(filter, strictPushDown, isES50)))
      .map{case((filter, (wasUsed, query))) => {
        if (!wasUsed) {
          unusedFilters += filter
        }
        query
      }}
      .filter{query => StringUtils.hasText(query)}.map{query => query}
    return (unusedFilters.toArray, queries)
  }

  // TODO Copied from DefaultSource -- reuse instead
  // string interpolation FTW
  private def translateFilter(filter: Filter, strictPushDown: Boolean, isES50: Boolean):(Boolean, String) = {
    // the pushdown can be strict - i.e. use only filters and thus match the value exactly (works with non-analyzed)
    // or non-strict meaning queries will be used instead that is the filters will be analyzed as well
    var filterHandled = true
    val queryClause = filter match {

      case EqualTo(attribute, value)            => {
        // if we get a null, translate it into a missing query (we're extra careful - Spark should translate the equals into isMissing anyway)
        if (value == null || value == None || value == ()) {
          if (isES50) {
            s"""{"bool":{"must_not":{"exists":{"field":"$attribute"}}}}"""
          }
          else {
            s"""{"missing":{"field":"$attribute"}}"""
          }
        }

        if (strictPushDown) s"""{"term":{"$attribute":${extract(value)}}}"""
        else {
          if (isES50) {
            s"""{"match":{"$attribute":${extract(value)}}}"""
          }
          else {
            s"""{"query":{"match":{"$attribute":${extract(value)}}}}"""
          }
        }
      }
      case GreaterThan(attribute, value)        => s"""{"range":{"$attribute":{"gt" :${extract(value)}}}}"""
      case GreaterThanOrEqual(attribute, value) => s"""{"range":{"$attribute":{"gte":${extract(value)}}}}"""
      case LessThan(attribute, value)           => s"""{"range":{"$attribute":{"lt" :${extract(value)}}}}"""
      case LessThanOrEqual(attribute, value)    => s"""{"range":{"$attribute":{"lte":${extract(value)}}}}"""
      case In(attribute, values)                => {
        // when dealing with mixed types (strings and numbers) Spark converts the Strings to null (gets confused by the type field)
        // this leads to incorrect query DSL hence why nulls are filtered
        val filtered = values filter (_ != null)
        if (filtered.isEmpty) {
          return (false, "")
        }

        // further more, match query only makes sense with String types so for other types apply a terms query (aka strictPushDown)
        val attrType = schema(attribute).dataType
        val isStrictType = attrType match {
          case DateType |
               TimestampType => true
          case _             => false
        }

        if (!strictPushDown && isStrictType) {
          if (Utils.LOGGER.isDebugEnabled()) {
            Utils.LOGGER.debug(s"Attribute $attribute type $attrType not suitable for match query; using terms (strict) instead")
          }
        }

        if (strictPushDown || isStrictType) s"""{"terms":{"$attribute":${extractAsJsonArray(filtered)}}}"""
        else {
          if (isES50) {
            s"""{"bool":{"should":[${extractMatchArray(attribute, filtered)}]}}"""
          }
          else {
            s"""{"or":{"filters":[${extractMatchArray(attribute, filtered)}]}}"""
          }
        }
      }
      case IsNull(attribute)                    => {
        if (isES50) {
          s"""{"bool":{"must_not":{"exists":{"field":"$attribute"}}}}"""
        }
        else {
          s"""{"missing":{"field":"$attribute"}}"""
        }
      }
      case IsNotNull(attribute)                 => s"""{"exists":{"field":"$attribute"}}"""
      case And(left, right)                     => {
        if (isES50) {
          s"""{"bool":{"filter":[${translateFilter(left, strictPushDown, isES50)}, ${translateFilter(right, strictPushDown, isES50)}]}}"""
        }
        else {
          s"""{"and":{"filters":[${translateFilter(left, strictPushDown, isES50)}, ${translateFilter(right, strictPushDown, isES50)}]}}"""
        }
      }
      case Or(left, right)                      => {
        if (isES50) {
          s"""{"bool":{"should":[{"bool":{"filter":${translateFilter(left, strictPushDown, isES50)}}}, {"bool":{"filter":${translateFilter(right, strictPushDown, isES50)}}}]}}"""
        }
        else {
          s"""{"or":{"filters":[${translateFilter(left, strictPushDown, isES50)}, ${translateFilter(right, strictPushDown, isES50)}]}}"""
        }
      }
      case Not(filterToNeg)                     => {
        if (isES50) {
          s"""{"bool":{"must_not":${translateFilter(filterToNeg, strictPushDown, isES50)}}}"""
        }
        else {
          s"""{"not":{"filter":${translateFilter(filterToNeg, strictPushDown, isES50)}}}"""
        }
      }

      // the filter below are available only from Spark 1.3.1 (not 1.3.0)

      //
      // String Filter notes:
      //
      // the DSL will be quite slow (linear to the number of terms in the index) but there's no easy way around them
      // we could use regexp filter however it's a bit overkill and there are plenty of chars to escape
      // s"""{"regexp":{"$attribute":"$value.*"}}"""
      // as an alternative we could use a query string but still, the analyzed / non-analyzed is there as the DSL is slightly more complicated
      // s"""{"query":{"query_string":{"default_field":"$attribute","query":"$value*"}}}"""
      // instead wildcard query is used, with the value lowercased (to match analyzed fields)

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringStartsWith") => {
        val arg = {
          val x = f.productElement(1).toString()
          if (!strictPushDown) x.toLowerCase(Locale.ROOT) else x
        }
        if (isES50) {
          s"""{"wildcard":{"${f.productElement(0)}":"$arg*"}}"""
        }
        else {
          s"""{"query":{"wildcard":{"${f.productElement(0)}":"$arg*"}}}"""
        }
      }

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringEndsWith")   => {
        val arg = {
          val x = f.productElement(1).toString()
          if (!strictPushDown) x.toLowerCase(Locale.ROOT) else x
        }
        if (isES50) {
          s"""{"wildcard":{"${f.productElement(0)}":"*$arg"}}"""
        }
        else {
          s"""{"query":{"wildcard":{"${f.productElement(0)}":"*$arg"}}}"""
        }
      }

      case f:Product if isClass(f, "org.apache.spark.sql.sources.StringContains")   => {
        val arg = {
          val x = f.productElement(1).toString()
          if (!strictPushDown) x.toLowerCase(Locale.ROOT) else x
        }
        if (isES50) {
          s"""{"wildcard":{"${f.productElement(0)}":"*$arg*"}}"""
        }
        else {
          s"""{"query":{"wildcard":{"${f.productElement(0)}":"*$arg*"}}}"""
        }
      }

      // the filters below are available only from Spark 1.5.0

      case f:Product if isClass(f, "org.apache.spark.sql.sources.EqualNullSafe")    => {
        val arg = extract(f.productElement(1))
        if (strictPushDown) s"""{"term":{"${f.productElement(0)}":$arg}}"""
        else {
          if (isES50) {
            s"""{"match":{"${f.productElement(0)}":$arg}}"""
          }
          else {
            s"""{"query":{"match":{"${f.productElement(0)}":$arg}}}"""
          }
        }
      }

      case _                                                                        => {
        filterHandled = false
        ""
      }
    }
    return (filterHandled, queryClause)
  }

  private def isClass(obj: Any, className: String) = {
    className.equals(obj.getClass().getName())
  }

  private def extract(value: Any):String = {
    extract(value, true, false)
  }

  private def extractAsJsonArray(value: Any):String = {
    extract(value, true, true)
  }

  private def extractMatchArray(attribute: String, ar: Array[Any]):String = {
    // use a set to avoid duplicate values
    // especially since Spark conversion might turn each user param into null
    val numbers = LinkedHashSet.empty[AnyRef]
    val strings = LinkedHashSet.empty[AnyRef]

    // move numbers into a separate list for a terms query combined with a bool
    for (i <- ar) i.asInstanceOf[AnyRef] match {
      case null     => // ignore
      case n:Number => numbers += extract(i, false, false)
      case _        => strings += extract(i, false, false)
    }
    val cfg = new MapBackedSettings(backingMap)
    if (numbers.isEmpty) {
      if (strings.isEmpty) {
        StringUtils.EMPTY
      } else {
        if (isEs50(cfg)) {
          s"""{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}"""
        }
        else {
          s"""{"query":{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}}"""
        }
      }
    } else {
      // translate the numbers into a terms query
      val str = s"""{"terms":{"$attribute":${numbers.mkString("[", ",", "]")}}}"""
      if (strings.isEmpty){
        str
        // if needed, add the strings as a match query
      } else str + {
        if (isEs50(cfg)) {
          s""",{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}"""
        }
        else {
          s""",{"query":{"match":{"$attribute":${strings.mkString("\"", " ", "\"")}}}}"""
        }
      }
    }
  }

  private def extract(value: Any, inJsonFormat: Boolean, asJsonArray: Boolean):String = {
    // common-case implies primitives and String so try these before using the full-blown ValueWriter
    value match {
      case null           => "null"
      case u: Unit        => "null"
      case b: Boolean     => b.toString
      case by: Byte       => by.toString
      case s: Short       => s.toString
      case i: Int         => i.toString
      case l: Long        => l.toString
      case f: Float       => f.toString
      case d: Double      => d.toString
      case bd: BigDecimal  => bd.toString
      case _: Char        |
           _: String      |
           _: Array[Byte]  => if (inJsonFormat) StringUtils.toJsonString(value.toString) else value.toString()
      // handle Timestamp also
      case dt: Date        => {
        val cal = Calendar.getInstance()
        cal.setTime(dt)
        val str = DatatypeConverter.printDateTime(cal)
        if (inJsonFormat) StringUtils.toJsonString(str) else str
      }
      case ar: Array[Any] =>
        if (asJsonArray) (for (i <- ar) yield extract(i, true, false)).distinct.mkString("[", ",", "]")
        else (for (i <- ar) yield extract(i, false, false)).distinct.mkString("\"", " ", "\"")
      // new in Spark 1.4
      case utf if (isClass(utf, "org.apache.spark.sql.types.UTF8String")
        // new in Spark 1.5
        || isClass(utf, "org.apache.spark.unsafe.types.UTF8String"))
      => if (inJsonFormat) StringUtils.toJsonString(utf.toString()) else utf.toString()
      case a: AnyRef      => {
        val storage = new FastByteArrayOutputStream()
        val generator = new JacksonJsonGenerator(storage)
        valueWriter.write(a, generator)
        generator.flush()
        generator.close()
        storage.toString()
      }
    }
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (filters != null && filters.size > 0) {
      val settings = new MapBackedSettings(backingMap)
      if (Utils.isPushDown(settings)) {
        if (Utils.LOGGER.isDebugEnabled()) {
          Utils.LOGGER.debug(s"Pushing down filters ${filters.mkString("[", ",", "]")}")
        }
        val filterInfo: (Array[Filter], Array[String]) = createDSLFromFilters(filters, Utils.isPushDownStrict(settings), isEs50(settings))
        filterStrings = filterInfo._2
        if (Utils.LOGGER.isTraceEnabled()) {
          Utils.LOGGER.trace(s"Transformed filters into DSL ${filterStrings.mkString("[", ",", "]")}")
        }
        backingMap.put(InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS , IOUtils.serializeToBase64(filterStrings))
        _pushedFilters = filters.filter(filter => !filterInfo._1.contains(filter))
        return filterInfo._1
      } else {
        return filters
      }
    } else {
      return filters
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    val groupByColumns = aggregation.groupByColumns()
    val aggregationExpressions = aggregation.aggregateExpressions()
    val fields = new Array[StructField](groupByColumns.size + aggregationExpressions.size)
    groupByColumns.zipWithIndex.foreach{case (col, index) => {
      col.fieldNames().zipWithIndex.foreach{case (fieldName, innerIndex) => {
        fields((index + 1) * innerIndex) = StructField(fieldName, StringType, false, new MetadataBuilder().build())
        groupBys.add(fieldName)
      }}
    }}

    aggregationExpressions.zipWithIndex.foreach { case (aggregateFunc, index) => {
      var fieldKey: String = null
      var fieldName: String = null
      var elasticfieldType: FieldType = null
      var fieldType: DataType = null
      var aggType: String = null
      aggregateFunc match {
        case count: Count => {
          //TODO: use isDistinct
          fieldName = count.column.fieldNames()(0) //TODO: what if there are multiple?
          fieldKey = "COUNT(" + fieldName + ")"
          elasticfieldType = FieldType.LONG //TODO: look this up from the schema?
          fieldType = LongType
          aggType = "value_count"
        }
        case countStar: CountStar => {
          fieldName = updatedSchema.fieldNames(0)
          fieldKey = "COUNT(\"*\")"
          elasticfieldType = FieldType.LONG //TODO: look this up from the schema?
          fieldType = LongType
          aggType = "value_count"
        }
        case max: Max => {
          fieldName = max.column.fieldNames()(0) //TODO: what if there are multiple?
          fieldKey = "MAX(" + fieldName + ")"
          elasticfieldType = FieldType.INTEGER //TODO: look this up from the schema?
          fieldType = IntegerType
          aggType = "max"
        }
        case min: Min => {
          fieldName = min.column.fieldNames()(0) //TODO: what if there are multiple?
          fieldKey = "MIN(" + fieldName + ")"
          elasticfieldType = FieldType.INTEGER //TODO: look this up from the schema?
          fieldType = IntegerType
          aggType = "min"
        }
        case sum: Sum => {
          //TODO: use isDistinct
          fieldName = sum.column.fieldNames()(0) //TODO: what if there are multiple?
          fieldKey = "SUM(" + fieldName + ")"
          elasticfieldType = FieldType.INTEGER //TODO: look this up from the schema?
          fieldType = IntegerType
          aggType = "sum"
        }
      }
      aggregations.put(fieldKey, new CompositeAggReader.AggInfo(fieldKey, fieldName, elasticfieldType, aggType))
      val nullable = false
      val metadata = new MetadataBuilder()
      fields(index + groupByColumns.size) = StructField(fieldKey, fieldType, nullable, metadata.build())
    }
      updatedSchema = StructType(fields)
    }
    true
  }
}