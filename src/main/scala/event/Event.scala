package event

import java.time.LocalDateTime

import input.Instance
import model.Condition

import scala.collection.mutable.ArrayBuffer

abstract class Event
case class InstanceEvent(instance: Instance) extends Event
case class PredictionEvent(prediction: Double, trueClass: Double) extends Event
abstract class RuleEvent extends Event
case class NewConditionEvent(condition: Condition, prediction: Double, ruleId: Int) extends RuleEvent
case class NewRuleBodyEvent(conditions: ArrayBuffer[Condition], prediction: Double, ruleId: Int) extends RuleEvent
abstract class MetricsEvent extends Event {def ruleId: Int}
case class NewMetricsEvent(ruleId: Int) extends MetricsEvent
case class MetricsUpdateEvent(ruleId: Int, instance: Instance) extends MetricsEvent



