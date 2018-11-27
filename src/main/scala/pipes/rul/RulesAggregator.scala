package pipes.rul

import event.Event
import input.Instance
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

import scala.util.Random

class RulesAggregator(outputTag: OutputTag[Event]) extends ProcessFunction[Event, Event] {

  var i = 0

  override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
    println("Process: " + event)
    out.collect(new Event("Prediction", 1.0, 1.0))

//    if (value.info.equals("Instance")) {
//      Thread.sleep(10)
//      i = i + 1
//      out.collect(Event("Prediction " + i))
//    }
//    //else if (value.info == "NewCondition") println("Received: " + value)
//    if (Random.nextDouble() < 0.5) ctx.output(outputTag, Event("UpdateRule"))
  }

}

