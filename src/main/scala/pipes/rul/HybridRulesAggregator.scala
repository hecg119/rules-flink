package pipes.rul

import event._
import input.{Instance, StreamHeader}
import model.{Condition, RuleBody}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import utils.Rules

import scala.collection.mutable

class HybridRulesAggregator(streamHeader: StreamHeader, extMin: Int, metricsUpdateTag: OutputTag[MetricsEvent], forwardedInstancesTag: OutputTag[Event])
  extends BroadcastProcessFunction[Event, Event, Event] {

  val clsNum: Int = streamHeader.clsNum()
  val rules: mutable.Map[Int, RuleBody] = mutable.Map()

  var i = 0
  var u = 0

  override def processElement(event: Event, ctx: BroadcastProcessFunction[Event, Event, Event]#ReadOnlyContext, out: Collector[Event]): Unit = {
    event match {
      case e: InstanceEvent =>
        val instance = e.instance
        requestMetricsUpdate(instance, ctx)
        out.collect(PredictionEvent(instance.classLbl, predict(instance)))

      case _ => throw new Error(s"This operator handles only InstanceEvent. Received: ${event.getClass}")
    }
  }

  override def processBroadcastElement(event: Event, ctx: BroadcastProcessFunction[Event, Event, Event]#Context, collector: Collector[Event]): Unit = {
    event match {
      case e: NewConditionEvent => updateRule(e.ruleId, e.condition, e.prediction)
      case e: NewRuleBodyEvent => rules.put(e.ruleId, new RuleBody(e.conditions, e.prediction))
      case _ => throw new Error(s"This operator handles only NewConditionEvent or NewRuleBodyEvent (broadcast). Received: ${event.getClass}")
    }
  }

  def requestMetricsUpdate(instance: Instance, ctx: BroadcastProcessFunction[Event, Event, Event]#ReadOnlyContext): Unit = {
    val rulesToUpdate = rules
      .filter(_._2.cover(instance))
      .keys

    rulesToUpdate.foreach((ruleId: Int) => ctx.output(metricsUpdateTag, MetricsUpdateEvent(ruleId, instance)))

    if (rulesToUpdate.isEmpty) {
      ctx.output(forwardedInstancesTag, InstanceEvent(instance))
    }
  }

  def predict(instance: Instance): Double = {
    Rules.classify(instance, rules.values.toArray, clsNum)
  }

  def updateRule(ruleId: Int, newCondition: Condition, newPrediction: Double): Unit = {
    rules(ruleId).updateConditions(newCondition)
    rules(ruleId).updatePrediction(newPrediction)
  }

  def print(): Unit = {
    Rules.printRules(rules.values.toArray, streamHeader)
  }

}
