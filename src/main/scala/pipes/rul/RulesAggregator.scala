package pipes.rul

import java.time.LocalDateTime

import event.Event
import input.{Instance, StreamHeader}
import model.{Condition, DefaultRule, RuleBody}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import utils.Rules

import scala.collection.mutable.ArrayBuffer

class RulesAggregator(streamHeader: StreamHeader, extMin: Int, metricsUpdateTag: OutputTag[Event]) extends ProcessFunction[Event, Event] {

  val clsNum: Int = streamHeader.clsNum()

  val rules: ArrayBuffer[RuleBody] = ArrayBuffer()
  val defaultRule: DefaultRule = new DefaultRule(streamHeader.attrNum(), streamHeader.clsNum(), extMin)

  var i = 0
  var u = 0

  override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
    if (event.getType.equals("Instance")) {
      println("I")
      val instance = event.instance
      updateMetrics(instance, ctx)
      out.collect(new Event("Prediction", instance.classLbl, predict(instance)))
    }
    else if (event.getType.equals("NewCondition")) {
      println("UUUU")
      updateRule(event.ruleId, event.condition, event.prediction)
    }
    else throw new Error(s"This operator handles only Instance and NewCondition events. Received: ${event.getType}")
  }

  def updateMetrics(instance: Instance, ctx:ProcessFunction[Event, Event]#Context): Unit = {
    val rulesToUpdate = rules
      .zipWithIndex
      .filter(_._1.cover(instance))
      .map(_._2)

    rulesToUpdate.foreach((ruleId: Int) => ctx.output(metricsUpdateTag, new Event("UpdateRule", ruleId, instance)))

    if (rulesToUpdate.isEmpty && defaultRule.update(instance)) {
      ctx.output(metricsUpdateTag, new Event("NewRule", rules.length))
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

