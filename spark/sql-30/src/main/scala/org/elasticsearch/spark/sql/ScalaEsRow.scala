/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.spark.sql

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row

private[spark] class ScalaEsRow(private[spark] val rowOrder: Seq[String]) extends Row {

  lazy private[spark] val values: ArrayBuffer[Any] = ArrayBuffer.fill(rowOrder.size)(null)

  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)

  def iterator = values.iterator

  override def length = values.size

  override def apply(i: Int) = values(i)

  override def get(i: Int): Any = values(i)
  
  override def isNullAt(i: Int) = values(i) == null

  override def getInt(i: Int): Int = getAs[Number](i).intValue()

  override def getLong(i: Int): Long = getAs[Number](i).longValue()

  override def getDouble(i: Int): Double = getAs[Number](i).doubleValue()

  override def getFloat(i: Int): Float = getAs[Number](i).floatValue()

  override def getBoolean(i: Int): Boolean = getAs[Boolean](i)

  override def getShort(i: Int): Short = getAs[Short](i)

  override def getByte(i: Int): Byte = getAs[Byte](i)

  override def getString(i: Int): String = get(i).toString()
  
  def copy() = this

  override def toSeq = values.toSeq
}