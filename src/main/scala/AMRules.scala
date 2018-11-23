import scala.collection.mutable.ArrayBuffer

class AMRules() {

  val attrNum: Int = 10
  val clsNum: Int = 10

  var rules: ArrayBuffer[Rule] = ArrayBuffer(new Rule())
  var rulesStats: ArrayBuffer[RuleStatistics] = ArrayBuffer(new RuleStatistics(attrNum, clsNum))

  val EXT_MIN: Int = 100
  val DELTA: Double = 1 - 0.95
  val R: Double = 1.0

  def update(instance: Instance): Unit = {
    var covered = false

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      if (isCovered(instance, rule)) {
        updateStatistics(ruleId, instance) // dist
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
        rules.append(rules(0).copy())
        rules(0) = new Rule() // todo: clearing?
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

  def updateStatistics(ruleId: Int, instance: Instance): Unit = {
    val ruleStats = rulesStats(ruleId)
    val classAttributesMetrics = ruleStats.classesAttributesMetrics(instance.classLbl)

    ruleStats.count = ruleStats.count + 1
    classAttributesMetrics.count = classAttributesMetrics.count + 1

    for ((attVal, i) <- instance.attributes.zipWithIndex) {
      val classAttributeMetrics = classAttributesMetrics.attributesMetrics(i)

      classAttributeMetrics.count = classAttributeMetrics.count + 1

      val lastMean = classAttributeMetrics.mean
      val lastStd = classAttributeMetrics.std
      val count = classAttributeMetrics.count

      classAttributeMetrics.mean = classAttributeMetrics.mean + ((lastMean - attVal) / classAttributeMetrics.count)
      classAttributeMetrics.std = ((count - 2) / (count - 1)) * math.pow(lastStd, 2) + (1 / count) * math.pow(attVal - lastMean, 2)

      classAttributesMetrics.attributesMetrics(i) = classAttributeMetrics
    }

    ruleStats.classesAttributesMetrics(instance.classLbl) = classAttributesMetrics
    rulesStats(ruleId) = ruleStats
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
    rulesStats(ruleId) = new RuleStatistics(clsNum, attrNum) // todo: ok?
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
  case class Rule(conditions: ArrayBuffer[Condition]) {
    def this() = this(ArrayBuffer())
  }
  case class RuleStatistics(attrNum: Int, clsNum: Int, var classesAttributesMetrics: ArrayBuffer[ClassAttributesMetrics], var count: Int) {
    def this(attrNum: Int, clsNum: Int) = this(
      attrNum,
      clsNum,
      ArrayBuffer.fill(clsNum)(ClassAttributesMetrics(ArrayBuffer.fill(attrNum)(AttributeMetrics(0, 0, 0)), 0)),
      0
    )
  }
  case class ClassAttributesMetrics(var attributesMetrics: ArrayBuffer[AttributeMetrics], var count: Int)
  case class AttributeMetrics(var mean: Double, var std: Double, var count: Int)
  case class Instance(attributes: ArrayBuffer[Double], classLbl: Int)
}
