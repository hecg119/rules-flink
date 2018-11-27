package pipes.rul

import event.Event
import input.Instance
import model.RuleMetrics
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class PartialRulesProcessor(attrNum: Int, clsNum: Int, extMin: Int) extends FlatMapFunction[Event, Event] {

  var i = 0
  val rulesStats: mutable.Map[Int, RuleMetrics] = mutable.Map()

  override def flatMap(event: Event, collector: Collector[Event]): Unit = {

    if (event.getType.equals("NewRule")) rulesStats.put(event.ruleId, new RuleMetrics(attrNum, clsNum))
    else if (event.getType.equals("UpdateRule")) {
      val ruleId = event.ruleId
      if (!rulesStats.contains(ruleId)) {
        throw new Error(s"This partition maintains rules: ${rulesStats.keys.mkString(" ")}. Received: $ruleId.")
      }

      updateRule(ruleId, event.instance, collector)

    } else throw new Error(s"This operator handles only NewRule and UpdateRule events. Received: ${event.getType}")
  }

  private def updateRule(ruleId: Int, instance: Instance, collector: Collector[Event]): Unit = {
    rulesStats(ruleId).updateStatistics(instance)

    if (rulesStats(ruleId).count > extMin) {
      val extension = rulesStats(ruleId).expandRule()
      if (extension != null) {
        collector.collect(new Event("NewCondition", extension._1, extension._2, ruleId))
      }
    }
  }

}
