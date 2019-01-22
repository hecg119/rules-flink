package pipes.rul

import event._
import input.{Instance, StreamHeader}
import model.RuleMetrics
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class PartialMetricsProcessor(streamHeader: StreamHeader, extMin: Int) extends RichFlatMapFunction[(MetricsEvent, Int), Event] {

  val attrNum: Int = streamHeader.attrNum()
  val clsNum: Int = streamHeader.clsNum()
  val rulesStats: mutable.Map[Int, RuleMetrics] = mutable.Map()

  var i = 0

  override def flatMap(eventWithId: (MetricsEvent, Int), out: Collector[Event]): Unit = {
    val event = eventWithId._1

    event match {
      case e: NewMetricsEvent => rulesStats.put(e.ruleId, new RuleMetrics(attrNum, clsNum))

      case e: MetricsUpdateEvent =>
        val ruleId = e.ruleId

        if (!rulesStats.contains(ruleId)) {
          throw new Error(s"This partition maintains rules: ${rulesStats.keys.mkString(" ")}. Received: $ruleId.")
        }

        updateRule(ruleId, e.instance, out)

      case _ => throw new Error(s"This operator handles only NewRuleMetricsEvent and RuleMetricsUpdateEvent. Received: ${event.getClass}")
    }
  }

  private def updateRule(ruleId: Int, instance: Instance, out: Collector[Event]): Unit = {
    rulesStats(ruleId).updateStatistics(instance)

    if (rulesStats(ruleId).count > extMin) {
      val extension = rulesStats(ruleId).expandRule()
      if (extension != null) {
        out.collect(NewConditionEvent(extension._1, extension._2, ruleId))
      }
    }
  }

}
