package utils

import input.{Instance, StreamHeader}
import model.RuleBody

import scala.collection.mutable.ArrayBuffer

object Rules {

  def printRules(rules: Array[RuleBody], streamHeader: StreamHeader): Unit = {
    println(s"\nCurrent AMRules [${rules.length}]:")

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      var cstr: ArrayBuffer[String] = ArrayBuffer()

      for (c <- rule.conditions) {
        cstr += s"${streamHeader.columnName(c.attributeIdx)} ${c.relation} ${c.value}"
      }

      println(s"Rule $ruleId: IF ${cstr.mkString(" AND ")} THEN ${rule.prediction}")
    }
    println
  }

  def classify(instance: Instance, rules: Array[RuleBody], clsNum: Int): Double = {
    val votes = ArrayBuffer.fill(clsNum)(0)

    for (rule <- rules) {
      if (rule.cover(instance)) {
        val clsIdx = rule.prediction.toInt
        votes(clsIdx) = votes(clsIdx) + 1
      }
    }

    votes.indices.maxBy(votes)
  }

}
