package event

import java.time.LocalDateTime

import input.Instance
import model.Condition

import scala.collection.mutable.ArrayBuffer

abstract class Event
case class InstanceEvent(instance: Instance) extends Event
case class PredictionEvent(prediction: Double, trueClass: Double) extends Event
// trait RuleBodyEvent
case class NewConditionEvent(condition: Condition, prediction: Double, ruleId: Int) extends Event
case class NewRuleBodyEvent(conditions: ArrayBuffer[Condition], prediction: Double, ruleId: Int) extends Event
abstract class MetricsEvent extends Event {def ruleId: Int}
case class NewRuleMetricsEvent(ruleId: Int) extends MetricsEvent
case class RuleMetricsUpdateEvent(ruleId: Int, instance: Instance) extends MetricsEvent



