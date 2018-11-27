package model

import input.Instance

class DefaultRule(attrNum: Int, clsNum: Int, extMin: Int, var ruleBody: RuleBody, var ruleMetrics: RuleMetrics)
  extends Serializable {

  def this(attrNum: Int, clsNum: Int, extMin: Int) = {
    this(attrNum, clsNum, extMin, new RuleBody(), new RuleMetrics(attrNum, clsNum))
  }

  def update(instance: Instance): Boolean = {
    ruleMetrics.updateStatistics(instance)

    if (ruleMetrics.count > extMin) {
      val expansion: (Condition, Double) = ruleMetrics.expandRule()

      if (expansion != null) {
        ruleBody.updateConditions(expansion._1)
        ruleBody.updatePrediction(expansion._2.toDouble)
        return true
      }
    }
    false
  }

  def reset(): Unit = {
    ruleBody = new RuleBody()
    ruleMetrics = new RuleMetrics(attrNum, clsNum)
  }

}
