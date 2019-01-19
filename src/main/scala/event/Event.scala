package event

import java.time.LocalDateTime

import input.Instance
import model.Condition

// todo: very ugly, change to common interface + case/match

class Event(eventType: String) extends Serializable {

  var instance: Instance = _
  var condition: Condition = _
  var ruleId: Int = _
  var trueClass: Double = _
  var prediction: Double = _
  var timestamp: String = LocalDateTime.now().toString

  def getType: String = eventType

  def this(eventType: String, instance: Instance) = { // Instance
    this(eventType)
    this.instance = instance
  }

  def this(eventType: String, condition: Condition, prediction: Double, ruleId: Int) = { // NewCondition, NewRule (from DefaultRule)
    this(eventType)
    this.condition = condition
    this.ruleId = ruleId
    this.prediction = prediction
  }

  def this(eventType: String, ruleId: Int) = { // NewRule
    this(eventType)
    this.ruleId = ruleId
  }

  def this(eventType: String, ruleId: Int, instance: Instance) = { // UpdateRule
    this(eventType)
    this.ruleId = ruleId
    this.instance = instance
  }

  def this(eventType: String, trueClass: Double, prediction: Double) = { // Prediction
    this(eventType)
    this.trueClass = trueClass
    this.prediction = prediction
  }

}

