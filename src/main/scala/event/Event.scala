package event

import input.Instance
import model.Condition

class Event(eventType: String) extends Serializable {

  private var instance: Instance = _
  private var condition: Condition = _
  private var ruleId: Int = _

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

  // todo: change to common interface + case/match
}

