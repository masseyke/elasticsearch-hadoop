package org.elasticsearch.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

case class ElasticsearchWrite() extends Write {

  override def toBatch = {
    new BatchWrite() {
      override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
        println("Getting batch writer factory!!")
        (_: Int, _: Long) => {
          println("In createWriter")
          new DataWriter[InternalRow] {
            override def write(t: InternalRow): Unit = {
              println("Writing!!" + t.toString)
            }

            override def commit(): WriterCommitMessage = {
              println("Committing!!!")
              new WriterCommitMessage {}
            }

            override def abort(): Unit = {
              println("Aborting!!")
            }

            override def close(): Unit = {
              println("Closing!!")
            }
          }
        }
      }

      override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
        println("Committing!!")
      }

      override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
        println("Aborting!!")
      }
    }
  }
}