package model.pipes.ens

import input.Instance
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ReplicateInstance(numPartitions: Int) extends FlatMapFunction[Instance, (Int, Instance)]{

  override def flatMap(instance: Instance, collector: Collector[(Int, Instance)]): Unit = {
    val instances: Array[(Instance, Int)] = Array.fill(numPartitions)(instance).zipWithIndex
    for (instanceTuple: (Instance, Int) <- instances) {
      collector.collect(instanceTuple.swap)
    }
  }

}
