package model.pipes.ens

import org.apache.flink.api.common.functions.Partitioner

class IntegerPartitioner(numPartitions: Int) extends Partitioner[Int] {

  override def partition(k: Int, i: Int): Int = {
    k % numPartitions
  }

}
