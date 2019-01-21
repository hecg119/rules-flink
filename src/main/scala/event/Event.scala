package event

import java.time.LocalDateTime

import input.Instance
import model.Condition

import scala.collection.mutable.ArrayBuffer


//class Event(eventType: String) extends Serializable {
//
//  var instance: Instance = _
//  var condition: Condition = _
//  var ruleId: Int = _
//  var trueClass: Double = _
//  var prediction: Double = _
//  var timestamp: String = LocalDateTime.now().toString
//
//  def getType: String = eventType
//
//  def this(eventType: String, instance: Instance) = { // Instance
//    this(eventType)
//    this.instance = instance
//  }
//
//  def this(eventType: String, condition: Condition, prediction: Double, ruleId: Int) = { // NewCondition, NewRule (from DefaultRule)
//    this(eventType)
//    this.condition = condition
//    this.ruleId = ruleId
//    this.prediction = prediction
//  }
//
//  def this(eventType: String, ruleId: Int) = { // NewRule
//    this(eventType)
//    this.ruleId = ruleId
//  }
//
//  def this(eventType: String, ruleId: Int, instance: Instance) = { // UpdateRule
//    this(eventType)
//    this.ruleId = ruleId
//    this.instance = instance
//  }
//
//  def this(eventType: String, trueClass: Double, prediction: Double) = { // Prediction
//    this(eventType)
//    this.trueClass = trueClass
//    this.prediction = prediction
//  }
//
//}

abstract class Event
case class InstanceEvent(instance: Instance) extends Event
case class PredictionEvent(prediction: Double, trueClass: Double) extends Event
// trait RuleBodyEvent
case class NewConditionEvent(condition: Condition, prediction: Double, ruleId: Int) extends Event
case class NewRuleBodyEvent(conditions: ArrayBuffer[Condition], prediction: Double, ruleId: Int) extends Event
abstract class MetricsEvent extends Event {def ruleId: Int}
case class NewRuleMetricsEvent(ruleId: Int) extends MetricsEvent
case class RuleMetricsUpdateEvent(ruleId: Int, instance: Instance) extends MetricsEvent



