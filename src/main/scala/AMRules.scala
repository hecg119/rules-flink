import scala.collection.mutable.ArrayBuffer

class AMRules {

  var rules: ArrayBuffer[Rule] = ArrayBuffer(Rule(ArrayBuffer()))
  var rulesStats: ArrayBuffer[RuleStatistics] = ArrayBuffer(RuleStatistics(0))

  val EXT_MIN: Int = 100

  def update(instance: Instance): Unit = {
    var covered = false

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      if (isCovered(instance, rule)) {
        updateStatistics(ruleId) // dist
        covered = true

        if (rulesStats(ruleId).count > EXT_MIN) {
          expandRule(ruleId) // dist?
        }
      }
    }

    if (!covered) {
      updateStatistics(0)

      if (rulesStats(0).count > EXT_MIN) {
        expandRule(0)
        rules.append(rules(0).copy())
        rules(0) = Rule(ArrayBuffer()) // todo: clearing?
      }
    }
  }

  def isCovered(instance: Instance, rule: Rule): Boolean = {
    for (c <- rule.conditions) {
      if (!conditionResolver(instance.attributes(c.attributeIdx), c.relation, c.value)) {
        return false
      }
    }
    true
  }

  def updateStatistics(ruleId: Int): Unit = {
    rulesStats.update(ruleId, RuleStatistics(rulesStats(ruleId).count + 1))
  }

  def expandRule(ruleId: Int): Unit = {
    // todo
  }

  def predict(): Double = {
    0.0
  }

  def conditionResolver(instanceAtt: Double, relation: String, ruleVal: Double): Boolean = relation match {
    case ">" => instanceAtt > ruleVal
    case "=" => instanceAtt == ruleVal
    case "<" => instanceAtt < ruleVal
  }

  case class Condition(attributeIdx: Int, relation: String, value: Double)
  case class Rule(conditions: ArrayBuffer[Condition])
  case class RuleStatistics(count: Int)
  case class Instance(attributes: ArrayBuffer[Double], classLbl: Double)
}
