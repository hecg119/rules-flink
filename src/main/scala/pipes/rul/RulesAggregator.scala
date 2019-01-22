package pipes.rul

import event._
import input.{Instance, StreamHeader}
import model.{Condition, DefaultRule, RuleBody}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import utils.Rules

import scala.collection.mutable.ArrayBuffer

class RulesAggregator(streamHeader: StreamHeader, extMin: Int, metricsUpdateTag: OutputTag[MetricsEvent]) extends ProcessFunction[Event, Event] {

  val clsNum: Int = streamHeader.clsNum()

  val rules: ArrayBuffer[RuleBody] = ArrayBuffer()
  val defaultRule: DefaultRule = new DefaultRule(streamHeader.attrNum(), streamHeader.clsNum(), extMin)

  var i = 0
  var u = 0

  override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
    event match {
      case e: InstanceEvent =>
        //println("I")
        val instance = e.instance
        updateMetrics(instance, ctx)
        out.collect(PredictionEvent(instance.classLbl, predict(instance)))

      case e: NewConditionEvent =>
        //println("UUUU")
        updateRule(e.ruleId, e.condition, e.prediction)

      case _ => throw new Error(s"This operator handles only InstanceEvent or NewConditionEvent. Received: ${event.getClass}")
    }
  }

  def updateMetrics(instance: Instance, ctx:ProcessFunction[Event, Event]#Context): Unit = {
    val rulesToUpdate = rules
      .zipWithIndex
      .filter(_._1.cover(instance))
      .map(_._2)

    rulesToUpdate.foreach((ruleId: Int) => ctx.output(metricsUpdateTag, MetricsUpdateEvent(ruleId, instance)))

    if (rulesToUpdate.isEmpty && defaultRule.update(instance)) {
      ctx.output(metricsUpdateTag, NewMetricsEvent(rules.length))
      rules.append(new RuleBody(defaultRule.ruleBody.conditions, defaultRule.ruleBody.prediction))
      defaultRule.reset()
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

