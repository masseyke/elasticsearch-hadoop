package org.elasticsearch.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

case class ElasticsearchWrite() extends Write {

  override def toBatch = {
    new BatchWrite() {
      override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
        (_: Int, _: Long) => {
          new DataWriter[InternalRow] {
            override def write(t: InternalRow): Unit = {
            }

            override def commit(): WriterCommitMessage = {
              new WriterCommitMessage {}
            }

            override def abort(): Unit = {
            }

            override def close(): Unit = {
            }
          }
        }
      }

      override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
      }

      override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
      }
    }
  }
}