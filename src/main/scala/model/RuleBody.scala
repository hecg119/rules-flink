package model

import input.Instance

import scala.collection.mutable.ArrayBuffer

class RuleBody(var conditions: ArrayBuffer[Condition], var prediction: Double) extends Serializable {

  def this() {
    this(new ArrayBuffer(), 0.0)
  }

  def updateConditions(newCondition: Condition): Unit = {
    conditions += newCondition
  }

  def updatePrediction(newPrediction: Double): Unit = {
    prediction = newPrediction
  }

  def cover(instance: Instance): Boolean = {
    for (c <- conditions) {
      if (!conditionResolver(instance.attributes(c.attributeIdx), c.relation, c.value)) {
        return false
      }
    }
    true
  }

  private def conditionResolver(instanceAtt: Double, relation: String, ruleVal: Double): Boolean = relation match {
    case ">" => instanceAtt > ruleVal
    case "=" => instanceAtt == ruleVal
    case "<=" => instanceAtt < ruleVal
  }

  def getPrediction: Double = prediction

  def getConditions: ArrayBuffer[Condition] = conditions

}

case class Condition(attributeIdx: Int, relation: String, value: Double)