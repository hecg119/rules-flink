package utils

import event.Event
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

class SimpleMerge extends CoProcessFunction[Event, Event, Event] {

  override def processElement1(event: Event, ctx: CoProcessFunction[Event, Event, Event]#Context, out: Collector[Event]): Unit = {
    out.collect(event)
  }

  override def processElement2(event: Event, ctx: CoProcessFunction[Event, Event, Event]#Context, out: Collector[Event]): Unit = {
    out.collect(event)
  }

}
