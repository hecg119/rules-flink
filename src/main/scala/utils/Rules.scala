package utils

import input.StreamHeader
import model.RuleBody

import scala.collection.mutable.ArrayBuffer

object Rules {

  def printRules(rules: Array[RuleBody], streamHeader: StreamHeader): Unit = {
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
