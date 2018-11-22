import scala.collection.mutable.ArrayBuffer

class AMRules() {

  val attrNum: Int = 10
  val clsNum: Int = 10

  var rules: ArrayBuffer[Rule] = ArrayBuffer(Rule(ArrayBuffer()))
  var rulesStats: ArrayBuffer[RuleStatistics] = ArrayBuffer(RuleStatistics(0, ArrayBuffer.fill(clsNum)(0)))

  val EXT_MIN: Int = 100
  val DELTA: Double = 1 - 0.95
  val R: Double = 1.0

  def update(instance: Instance): Unit = {
    var covered = false

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      if (isCovered(instance, rule)) {
        updateStatistics(ruleId, instance.classLbl) // dist
        covered = true

        if (rulesStats(ruleId).count > EXT_MIN) {
          expandRule(ruleId)
        }
      }
    }

    if (!covered) {
      updateStatistics(0, instance.classLbl)

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

  def updateStatistics(ruleId: Int, classLbl: Int): Unit = {
    rulesStats.update(ruleId, RuleStatistics(
      rulesStats(ruleId).count + 1,
      rulesStats(ruleId).clsCount.updated(classLbl, rulesStats(ruleId).clsCount(classLbl) + 1) // todo: count, min, max, for each class: mean, var, p(c); use NormalDistribution()
    ))
  }

  def expandRule(ruleId: Int): Unit = {
    val ruleEntropy = 44
    val bound = calcHoeffdingBound(rulesStats(ruleId).count)

    var bestSplit: (Condition, Double) = (Condition(-1, "", -1), 1.0)

    if (ruleEntropy > bound) {
      (0 to attrNum).foreach(aidx => { // dist
        val attrBestSplit = findBestSplit(aidx) // dist?

        if (attrBestSplit._2 < bestSplit._2) {
          bestSplit = attrBestSplit
        }
      })

      if (ruleEntropy - bestSplit._2 > bound) {
        rules(ruleId).conditions.append(bestSplit._1)
      }

      releaseStatistics(ruleId)
    }
  }

  def calcHoeffdingBound(n: Int): Double = math.sqrt(R * R * math.log(1 / DELTA) / (2 * n))

  def findBestSplit(aidx: Int): (Condition, Double) = {
    (Condition(-1, "", -1), 0.0) // todo
  }

  def releaseStatistics(ruleId: Int): Unit = {
    rulesStats.update(ruleId, RuleStatistics(0, ArrayBuffer.fill(clsNum)(0))) // todo: ok?
  }

  def predict(): Double = {
    0.0 // dist
  }

  def conditionResolver(instanceAtt: Double, relation: String, ruleVal: Double): Boolean = relation match {
    case ">" => instanceAtt > ruleVal
    case "=" => instanceAtt == ruleVal
    case "<=" => instanceAtt < ruleVal
  }

  case class Condition(attributeIdx: Int, relation: String, value: Double)
  case class Rule(conditions: ArrayBuffer[Condition])
  case class RuleStatistics(count: Int, clsCount: ArrayBuffer[Int])
  case class Instance(attributes: ArrayBuffer[Double], classLbl: Int)
}
