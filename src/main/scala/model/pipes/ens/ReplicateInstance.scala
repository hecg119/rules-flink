package model.pipes.ens

import input.Instance
import model.pipes.rul.Event
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class ReplicateInstance(numPartitions: Int) extends FlatMapFunction[Event, (Int, Event)]{

  override def flatMap(instance: Event, collector: Collector[(Int, Event)]): Unit = {
    val instances: Array[(Event, Int)] = Array.fill(numPartitions)(instance).zipWithIndex
    for (instanceTuple: (Event, Int) <- instances) {
      collector.collect(instanceTuple.swap)
    }
  }

}
