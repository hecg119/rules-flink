package pipes.rul

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.util.Collector

import scala.util.Random

class PartialRulesProcessor extends FlatMapFunction[(Int, Event), Event] {
  var i = 0

  override def flatMap(t: (Int, Event), collector: Collector[Event]): Unit = {
    i = i + 1
    collector.collect(Event("NewCondition " + i))
  }
}
