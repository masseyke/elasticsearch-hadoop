package org.elasticsearch.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class SingleValueRow(value: Long) extends InternalRow {
  override def numFields: Int = ???

  override def setNullAt(i: Int): Unit = ???

  override def update(i: Int, value: Any): Unit = ???

  override def copy(): InternalRow = ???

  override def isNullAt(ordinal: Int): Boolean = ???

  override def getBoolean(ordinal: Int): Boolean = ???

  override def getByte(ordinal: Int): Byte = ???

  override def getShort(ordinal: Int): Short = ???

  override def getInt(ordinal: Int): Int = value.toInt

  override def getLong(ordinal: Int): Long = value

  override def getFloat(ordinal: Int): Float = ???

  override def getDouble(ordinal: Int): Double = ???

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???

  override def getUTF8String(ordinal: Int): UTF8String = ???

  override def getBinary(ordinal: Int): Array[Byte] = ???

  override def getInterval(ordinal: Int): CalendarInterval = ???

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???

  override def getArray(ordinal: Int): ArrayData = ???

  override def getMap(ordinal: Int): MapData = ???

  override def get(ordinal: Int, dataType: DataType): AnyRef = ???
}