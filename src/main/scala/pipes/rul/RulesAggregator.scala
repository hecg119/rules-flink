package pipes.rul

import event.Event
import input.{Instance, StreamHeader}
import model.{Condition, DefaultRule, RuleBody}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

class RulesAggregator(streamHeader: StreamHeader, extMin: Int, outputTag: OutputTag[Event]) extends ProcessFunction[Event, Event] {

  val clsNum: Int = streamHeader.clsNum()

  val rules: ArrayBuffer[RuleBody] = ArrayBuffer()
  val defaultRule: DefaultRule = new DefaultRule(streamHeader.attrNum(), streamHeader.clsNum(), extMin)

  override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
    println("Process: " + event)

    if (event.getType.equals("Instance")) {
      val instance = event.instance
      update(instance, ctx)
      out.collect(new Event("Prediction", instance.classLbl, predict(instance)))
    }
    else if (event.getType.equals("NewCondition")) {
      updateRule(event.ruleId, event.condition, event.prediction)
    }
    else throw new Error(s"This operator handles only Instance and NewCondition events. Received: ${event.getType}")
  }

  def update(instance: Instance, ctx:ProcessFunction[Event, Event]#Context): Unit = {
    val rulesToUpdate = rules
      .zipWithIndex
      .filter(_._1.cover(instance))
      .map(_._2)

    rulesToUpdate.foreach((ruleId: Int) => ctx.output(outputTag, new Event("UpdateRule", ruleId)))

    if (rulesToUpdate.isEmpty && defaultRule.update(instance)) {
      ctx.output(outputTag, new Event("NewRule", rules.length - 1))
    }
  }

  def predict(instance: Instance): Double = { // todo: optimize/merge with seq
    val votes = ArrayBuffer.fill(clsNum)(0)

    for (rule <- rules) {
      if (rule.cover(instance)) {
        val clsIdx = rule.prediction.toInt
        votes(clsIdx) = votes(clsIdx) + 1
      }
    }

    votes.indices.maxBy(votes)
  }

  def updateRule(ruleId: Int, newCondition: Condition, newPrediction: Double): Unit = {
    rules(ruleId).updateConditions(newCondition)
    rules(ruleId).updatePrediction(newPrediction)
  }

}

