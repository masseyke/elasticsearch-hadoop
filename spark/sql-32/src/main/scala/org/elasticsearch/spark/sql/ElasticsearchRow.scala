package org.elasticsearch.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.sql.Timestamp

class ElasticsearchRow(wrappedRow: ScalaEsRow) extends InternalRow {
  override def numFields: Int = {
    wrappedRow.length
  }

  override def setNullAt(i: Int): Unit = ???

  override def update(i: Int, value: Any): Unit = ???

  override def copy(): InternalRow = ???

  override def isNullAt(i: Int): Boolean = wrappedRow.isNullAt(i)

  override def getBoolean(i: Int): Boolean = wrappedRow.getBoolean(i)

  override def getByte(i: Int): Byte = wrappedRow.getByte(i)

  override def getShort(i: Int): Short = wrappedRow.getShort(i)

  override def getInt(i: Int): Int =  {
    2
//    wrappedRow.getInt(i)
  }

  override def getLong(i: Int): Long = {
    val rawField = wrappedRow.get(i)
    if (rawField.isInstanceOf[Long]) {
      return rawField.asInstanceOf[Long]
    } else if (rawField.isInstanceOf[Timestamp]) {
      return rawField.asInstanceOf[Timestamp].getTime
    }
    ???
  }

  override def getFloat(i: Int): Float = wrappedRow.getFloat(i)

  override def getDouble(i: Int): Double = wrappedRow.getDouble(i)

  override def getDecimal(i: Int, i1: Int, i2: Int): Decimal = new Decimal().set(wrappedRow.getDecimal(i))

  override def getUTF8String(i: Int): UTF8String = {
    UTF8String.fromString(wrappedRow.getString(i))
  }

  override def getBinary(i: Int): Array[Byte] = ???

  override def getInterval(i: Int): CalendarInterval = ???

  override def getStruct(i: Int, i1: Int): InternalRow = {
    val struct = wrappedRow.getStruct(i)
    InternalRow.fromSeq(struct.toSeq)
  }

  override def getArray(i: Int): ArrayData = ???

  override def getMap(i: Int): MapData = ???

  override def get(i: Int, dataType: DataType): AnyRef = ???
}
