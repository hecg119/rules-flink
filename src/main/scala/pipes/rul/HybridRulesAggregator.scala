package pipes.rul

import event.Event
import input.{Instance, StreamHeader}
import model.{Condition, RuleBody}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import utils.Rules

import scala.collection.mutable.ArrayBuffer

class HybridRulesAggregator(streamHeader: StreamHeader, extMin: Int, metricsUpdateTag: OutputTag[Event]) extends BroadcastProcessFunction[Event, Event, Event] {

  val clsNum: Int = streamHeader.clsNum()
  val rules: ArrayBuffer[RuleBody] = ArrayBuffer()

  var i = 0
  var u = 0

  override def processBroadcastElement(event: Event, ctx: BroadcastProcessFunction[Event, Event, Event]#Context, collector: Collector[Event]): Unit = {
    if (event.getType.equals("NewCondition")) {
      updateRule(event.ruleId, event.condition, event.prediction)
    } else if (event.getType.equals("NewRule")) {
      rules.append(new RuleBody(ArrayBuffer(event.condition), event.prediction))
    }
    else throw new Error(s"This operator handles only NewCondition broadcast events. Received: ${event.getType}")
  }

  override def processElement(event: Event, ctx: BroadcastProcessFunction[Event, Event, Event]#ReadOnlyContext, out: Collector[Event]): Unit = {
    if (event.getType.equals("Instance")) {
      val instance = event.instance
      requestMetricsUpdate(instance, ctx)
      out.collect(new Event("Prediction", instance.classLbl, predict(instance)))
    }
  }

  def requestMetricsUpdate(instance: Instance, ctx: BroadcastProcessFunction[Event, Event, Event]#ReadOnlyContext): Unit = {
    val rulesToUpdate = rules
      .zipWithIndex
      .filter(_._1.cover(instance))
      .map(_._2)

    rulesToUpdate.foreach((ruleId: Int) => ctx.output(metricsUpdateTag, new Event("UpdateRule", ruleId, instance)))

    if (rulesToUpdate.isEmpty) {
      ctx.output(metricsUpdateTag, new Event("Instance", instance))
    }
  }

  def predict(instance: Instance): Double = {
    Rules.classify(instance, rules.toArray, clsNum)
  }

  def updateRule(ruleId: Int, newCondition: Condition, newPrediction: Double): Unit = {
    rules(ruleId).updateConditions(newCondition)
    rules(ruleId).updatePrediction(newPrediction)
  }

  def print(): Unit = {
    Rules.printRules(rules.toArray, streamHeader)
  }

}
