package model

import input.{Instance, StreamHeader}

import scala.collection.mutable.ArrayBuffer

class AMRules(streamHeader: StreamHeader) extends Serializable {

  private val attrNum: Int = streamHeader.attrNum()
  private val clsNum: Int = streamHeader.clsNum()

  private val rules: ArrayBuffer[RuleBody] = ArrayBuffer(new RuleBody())
  private val rulesStats: ArrayBuffer[RuleMetrics] = ArrayBuffer(new RuleMetrics(attrNum, clsNum))

  private val EXT_MIN: Int = 100

  def update(instance: Instance): Unit = {
    var covered = false

    for ((rule, ruleId) <- rules.zipWithIndex) {
      if (ruleId > 0 && rule.cover(instance)) {
        updateStatistics(ruleId, instance)
        covered = true

        if (rulesStats(ruleId).count > EXT_MIN) {
          expandRule(ruleId)
        }
      }
    }

    if (!covered) {
      updateStatistics(0, instance)

      if (rulesStats(0).count > EXT_MIN) {
        expandRule(0)
        rules.append(new RuleBody(rules(0).conditions, rules(0).prediction))
        rulesStats.append(new RuleMetrics(attrNum, clsNum))
        rules(0) = new RuleBody()
        rulesStats(0) = new RuleMetrics(attrNum, clsNum)
      }
    }
  }

  private def updateStatistics(ruleId: Int, instance: Instance): Unit = {
    rulesStats(ruleId).updateStatistics(instance)
  }

  private def expandRule(ruleId: Int): Unit = {
    val expansion: (Condition, Double) = rulesStats(ruleId).expandRule()
    if (expansion != null) {
      rules(ruleId).updateConditions(expansion._1)
      rules(ruleId).updatePrediction(expansion._2.toDouble)
    }
  }

  def predict(instance: Instance): Double = {
    val votes = ArrayBuffer.fill(clsNum)(0)

    for ((rule, ruleId) <- rules.zipWithIndex) {
      if (ruleId > 0 && rule.cover(instance)) {
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

