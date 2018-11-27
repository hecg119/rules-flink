package utils

import org.apache.flink.api.common.functions.Partitioner

class IntegerPartitioner(numPartitions: Int) extends Partitioner[Int] {

  override def partition(id: Int, numPartitions: Int): Int = {
    id % numPartitions
  }

}
