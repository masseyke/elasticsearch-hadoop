package org.elasticsearch.spark.sql

import org.elasticsearch.hadoop.cfg.Settings

import java.io.InputStream
import java.util.Properties
import scala.collection.mutable

class MapBackedSettings(backingMap: mutable.Map[String, String]) extends Settings {
  override def loadResource(location: String): InputStream = ???

  override def copy(): Settings = this

  override def getProperty(name: String): String = backingMap.getOrElse(name, null)

  override def setProperty(name: String, value: String): Unit = {
    println("Setting " + name + ": " + value)
    backingMap.addOne(name, value)
  }

  override def asProperties(): Properties = {
    val props = new Properties()
    for ((k,v) <- backingMap) props.put(k, v)
    props
  }
}
