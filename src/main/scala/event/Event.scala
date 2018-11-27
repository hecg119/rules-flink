package event

import input.Instance
import model.Condition

class Event(eventType: String) extends Serializable {

  var instance: Instance = _
  var condition: Condition = _
  var ruleId: Int = _
  var trueClass: Double = _
  var prediction: Double = _

  def getType: String = eventType

  def this(eventType: String, instance: Instance) = {
    this(eventType)
    this.instance = instance
  }

  def this(eventType: String, condition: Condition, ruleId: Int) = {
    this(eventType)
    this.condition = condition
    this.ruleId = ruleId
  }

  def this(eventType: String, ruleId: Int) = {
    this(eventType)
    this.ruleId = ruleId
  }

  def this(eventType: String, trueClass: Double, prediction: Double) = {
    this(eventType)
    this.trueClass = trueClass
    this.prediction = prediction
  }

  // todo: change to common interface + case/match
}

