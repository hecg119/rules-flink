package model

import input.{Instance, StreamHeader}
import utils.Rules

import scala.collection.mutable.ArrayBuffer

class SequentialAMR(streamHeader: StreamHeader, extMin: Int) extends Serializable {

  private val attrNum: Int = streamHeader.attrNum()
  private val clsNum: Int = streamHeader.clsNum()

  private val defaultRule: DefaultRule = new DefaultRule(attrNum, clsNum, extMin)
  private val rules: ArrayBuffer[RuleBody] = ArrayBuffer(new RuleBody())
  private val rulesStats: ArrayBuffer[RuleMetrics] = ArrayBuffer(new RuleMetrics(attrNum, clsNum))

  def update(instance: Instance): Unit = {
    var covered = false

    for ((rule, ruleId) <- rules.zipWithIndex) {
      if (rule.cover(instance)) {
        covered = true
        updateRule(ruleId, instance)
      }
    }

    if (!covered) {
      if (defaultRule.update(instance)) {
        rules.append(new RuleBody(defaultRule.ruleBody.conditions, defaultRule.ruleBody.prediction))
        rulesStats.append(new RuleMetrics(attrNum, clsNum))
        defaultRule.reset()
      }
    }
  }

  private def updateRule(ruleId: Int, instance: Instance): Unit = {
    rulesStats(ruleId).updateStatistics(instance)

    if (rulesStats(ruleId).count > extMin) {
      val expansion: (Condition, Double) = rulesStats(ruleId).expandRule()

      if (expansion != null) {
        rules(ruleId).updateConditions(expansion._1)
        rules(ruleId).updatePrediction(expansion._2.toDouble)
      }
    }
  }

  def predict(instance: Instance): Double = {
    Rules.classify(instance, rules.toArray, clsNum)
  }

  def print(): Unit = {
    Rules.printRules(rules.toArray, streamHeader)
  }

}

