package pipes.rul

import event.Event
import model.Condition
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.util.Collector

class PartialRulesProcessor extends FlatMapFunction[Event, Event] {

  var i = 0

  override def flatMap(event: Event, collector: Collector[Event]): Unit = {
    i = i + 1
    collector.collect(new Event("NewCondition", Condition(-1, "", -1), -1))
  }

}
