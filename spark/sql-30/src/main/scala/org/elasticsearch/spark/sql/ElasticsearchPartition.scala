package org.elasticsearch.spark.sql

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition
import org.elasticsearch.hadoop.rest.PartitionDefinition

class ElasticsearchPartition(idx: Int, val partitionDefinition: PartitionDefinition) extends Partition with InputPartition {
  override def index: Int = idx

  override def preferredLocations = partitionDefinition.getLocations
}