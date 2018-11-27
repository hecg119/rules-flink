package model

import input.{Instance, StreamHeader}

import scala.collection.mutable.ArrayBuffer

class AMRules(streamHeader: StreamHeader) extends Serializable {

  private val EXT_MIN: Int = 100

  private val attrNum: Int = streamHeader.attrNum()
  private val clsNum: Int = streamHeader.clsNum()

  private val defaultRule: DefaultRule = new DefaultRule(attrNum, clsNum, EXT_MIN)
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

    if (rulesStats(ruleId).count > EXT_MIN) {
      val expansion: (Condition, Double) = rulesStats(ruleId).expandRule()

      if (expansion != null) {
        rules(ruleId).updateConditions(expansion._1)
        rules(ruleId).updatePrediction(expansion._2.toDouble)
      }
    }
  }

  def predict(instance: Instance): Double = {
    val votes = ArrayBuffer.fill(clsNum)(0)

    for (rule <- rules) {
      if (rule.cover(instance)) {
        val clsIdx = rule.prediction.toInt
        votes(clsIdx) = votes(clsIdx) + 1
      }
    }

    votes.indices.maxBy(votes)
  }

  def print(): Unit = {
    println(s"\nCurrent AMRules [${rules.length - 1}]:")

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      var cstr: ArrayBuffer[String] = ArrayBuffer()

      for (c <- rule.conditions) {
        cstr += s"${streamHeader.columnName(c.attributeIdx)} ${c.relation} ${c.value}"
      }

      println(s"Rule $ruleId: IF ${cstr.mkString(" AND ")} THEN ${rule.prediction}")
    }
    println
  }

}

