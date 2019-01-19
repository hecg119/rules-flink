package pipes.rul

import event.Event
import input.StreamHeader
import model.DefaultRule
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

class DefaultRuleProcessor(streamHeader: StreamHeader, extMin: Int, metricsUpdateTag: OutputTag[Event]) extends ProcessFunction[Event, Event] {

  val defaultRule: DefaultRule = new DefaultRule(streamHeader.attrNum(), streamHeader.clsNum(), extMin)
  var rulesCount: Int = 0

  override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
    if (event.getType.equals("Instance")) {
      val instance = event.instance

      if (defaultRule.update(instance)) {
        rulesCount = rulesCount + 1

        ctx.output(metricsUpdateTag, new Event("NewRule", rulesCount)) // metrics
        out.collect(new Event("NewRule", defaultRule.ruleBody.conditions(0), defaultRule.ruleBody.prediction, rulesCount)) //body

        defaultRule.reset()
      }
    } else {
      throw new Error(s"This operator handles only UpdateRule events. Received: ${event.getType}")
    }
  }

}
