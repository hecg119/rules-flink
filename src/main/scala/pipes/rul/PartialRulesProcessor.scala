package pipes.rul

import event.Event
import input.{Instance, StreamHeader}
import model.RuleMetrics
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class PartialRulesProcessor(streamHeader: StreamHeader, extMin: Int) extends RichFlatMapFunction[(Event, Int), Event] {

  val attrNum: Int = streamHeader.attrNum()
  val clsNum: Int = streamHeader.clsNum()
  val rulesStats: mutable.Map[Int, RuleMetrics] = mutable.Map()

  var i = 0

  override def flatMap(eventWithId: (Event, Int), out: Collector[Event]): Unit = {
    val event = eventWithId._1

    if (event.getType.equals("NewRule")) {
      rulesStats.put(event.ruleId, new RuleMetrics(attrNum, clsNum))
    }
    else if (event.getType.equals("UpdateRule")) {
      val ruleId = event.ruleId

      if (!rulesStats.contains(ruleId)) {
        throw new Error(s"This partition maintains rules: ${rulesStats.keys.mkString(" ")}. Received: $ruleId.")
      }

      updateRule(ruleId, event.instance, out)

    } else {
      throw new Error(s"This operator handles only NewRule and UpdateRule events. Received: ${event.getType}")
    }
  }

  private def updateRule(ruleId: Int, instance: Instance, out: Collector[Event]): Unit = {
    rulesStats(ruleId).updateStatistics(instance)

    if (rulesStats(ruleId).count > extMin) {
      val extension = rulesStats(ruleId).expandRule()
      if (extension != null) {
        out.collect(new Event("NewCondition", extension._1, extension._2, ruleId))
      }
    }
  }

}
