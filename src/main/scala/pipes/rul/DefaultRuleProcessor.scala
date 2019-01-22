package pipes.rul

import event._
import input.StreamHeader
import model.DefaultRule
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

class DefaultRuleProcessor(streamHeader: StreamHeader, extMin: Int, metricsUpdateTag: OutputTag[MetricsEvent]) extends ProcessFunction[Event, Event] {

  val defaultRule: DefaultRule = new DefaultRule(streamHeader.attrNum(), streamHeader.clsNum(), extMin)
  var rulesCount: Int = 0

  override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
    event match {
      case e: InstanceEvent =>
        val instance = e.instance

        if (defaultRule.update(instance)) {
          rulesCount = rulesCount + 1

          ctx.output(metricsUpdateTag, NewMetricsEvent(rulesCount))
          out.collect(NewRuleBodyEvent(defaultRule.ruleBody.conditions, defaultRule.ruleBody.prediction, rulesCount))

          defaultRule.reset()

        }
      case _ => throw new Error(s"This operator handles only InstanceEvent. Received: ${event.getClass}")
    }
  }

}
