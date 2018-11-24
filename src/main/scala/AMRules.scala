import org.apache.commons.math3.distribution.NormalDistribution

import scala.collection.mutable.ArrayBuffer

class AMRules() extends Serializable {

  private val attrNum: Int = 10
  private val clsNum: Int = 10

  private val rules: ArrayBuffer[Rule] = ArrayBuffer(new Rule())
  private val rulesStats: ArrayBuffer[RuleStatistics] = ArrayBuffer(new RuleStatistics(attrNum, clsNum))

  private val EXT_MIN: Int = 100
  private val DELTA: Double = 1 - 0.95
  private val R: Double = 1.0
  private val SPLIT_GRAN: Double = 0.2

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

    // todo: remove rules if error > threshold
  }

  private def isCovered(instance: Instance, rule: Rule): Boolean = {
    for (c <- rule.conditions) {
      if (!conditionResolver(instance.attributes(c.attributeIdx), c.relation, c.value)) {
        return false
      }
    }
    true
  }

  private def updateStatistics(ruleId: Int, instance: Instance): Unit = {
    val ruleStats = rulesStats(ruleId)
    val classAttributesMetrics = ruleStats.classesAttributesMetrics(instance.classLbl)

    ruleStats.count = ruleStats.count + 1
    classAttributesMetrics.count = classAttributesMetrics.count + 1

    for ((attVal, i) <- instance.attributes.zipWithIndex) { // dist? + todo: distinguish numeric/nominal
      val attributeClassesMetrics = ruleStats.attributesClassesMetrics(i) // todo: get m - 3std / m + 3std instead
      if (attVal > attributeClassesMetrics.max) attributeClassesMetrics.max = attVal
      if (attVal < attributeClassesMetrics.min) attributeClassesMetrics.min = attVal

      val classAttributeMetrics = classAttributesMetrics.attributesMetrics(i)

      classAttributeMetrics.count = classAttributeMetrics.count + 1

      val lastMean = classAttributeMetrics.mean
      val lastStd = classAttributeMetrics.std
      val count = classAttributeMetrics.count

      classAttributeMetrics.mean = classAttributeMetrics.mean + ((lastMean - attVal) / classAttributeMetrics.count)
      classAttributeMetrics.std = ((count - 2) / (count - 1)) * math.pow(lastStd, 2) + (1 / count) * math.pow(attVal - lastMean, 2)

      classAttributesMetrics.attributesMetrics(i) = classAttributeMetrics // todo: use windowed stats
    }

    ruleStats.classesAttributesMetrics(instance.classLbl) = classAttributesMetrics
    rulesStats(ruleId) = ruleStats // todo: update error
  }

  private def expandRule(ruleId: Int): Unit = {
    val ruleEntropy = entropy(rulesStats(ruleId).classesAttributesMetrics.map(cm => cm.count.toDouble).toList)
    val bound = calcHoeffdingBound(rulesStats(ruleId).count)

    var bestSplit: (Condition, Double) = (Condition(-1, "", -1), 1.0)

    if (ruleEntropy > bound) {
      (0 to attrNum).foreach(attIdx => { // dist
        val attrBestSplit = findBestSplit(ruleId, attIdx) // dist?

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

  private def entropy(ps: List[Double]): Double = {
    ps.map(p => -p * math.log10(p) / math.log10(2.0)).sum
  }

  private def calcHoeffdingBound(n: Int): Double = math.sqrt(R * R * math.log(1 / DELTA) / (2 * n))

  private def findBestSplit(ruleId: Int, attIdx: Int): (Condition, Double) = { // todo: distinguish numeric/nominal
    val classesAttributeMetrics = rulesStats(ruleId).classesAttributesMetrics
    val min = rulesStats(ruleId).attributesClassesMetrics(attIdx).min
    val max = rulesStats(ruleId).attributesClassesMetrics(attIdx).max
    val step = (max - min) * SPLIT_GRAN

    var minEntropySplit: (Double, Double) = (Double.MaxValue, 0.0)
    var splitVal = min + step

    while (splitVal < max) {
      val splitEntropy = entropy((0 to clsNum).map((clsIdx) => {
        val mean = classesAttributeMetrics(clsIdx).attributesMetrics(attIdx).mean
        val std = classesAttributeMetrics(clsIdx).attributesMetrics(attIdx).std
        val p = new NormalDistribution(mean, std).cumulativeProbability(splitVal)
        - p * math.log10(p) / math.log10(2.0)
      }).toList)

      if (splitEntropy < minEntropySplit._1) minEntropySplit = (splitEntropy, splitVal)

      splitVal = splitVal + step
    }

    (Condition(attIdx, ">", minEntropySplit._2), minEntropySplit._1)
  }

  private def releaseStatistics(ruleId: Int): Unit = {
    rulesStats(ruleId) = new RuleStatistics(clsNum, attrNum) // todo: ok?
  }

  def predict(instance: Instance): Int = {
    val votes = ArrayBuffer.fill(clsNum)(0)

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      if (isCovered(instance, rule)) {
        val clsIdx = classifyInstance(ruleId, instance)
        votes(clsIdx) = votes(clsIdx) + 1
      }
    }

    votes.indices.maxBy(votes)
  }

  private def classifyInstance(ruleId: Int, instance: Instance): Int = {
    val metrics = rulesStats(ruleId).classesAttributesMetrics.map(cm => cm.count)
    metrics.indices.maxBy(metrics)
  }

  private def conditionResolver(instanceAtt: Double, relation: String, ruleVal: Double): Boolean = relation match {
    case ">" => instanceAtt > ruleVal
    case "=" => instanceAtt == ruleVal
    case "<=" => instanceAtt < ruleVal
  }

}

case class Rule(conditions: ArrayBuffer[Condition]) {
  def this() = this(ArrayBuffer())
}

case class Condition(attributeIdx: Int, relation: String, value: Double)

case class RuleStatistics(attrNum: Int, clsNum: Int, var classesAttributesMetrics: ArrayBuffer[ClassAttributesMetrics],
                          var attributesClassesMetrics: ArrayBuffer[AttributeClassesMetrics], var count: Int) {
  def this(attrNum: Int, clsNum: Int) = this(
    attrNum,
    clsNum,
    ArrayBuffer.fill(clsNum)(ClassAttributesMetrics(ArrayBuffer.fill(attrNum)(AttributeMetrics(0, 0, 0)), 0)),
    ArrayBuffer.fill(attrNum)(AttributeClassesMetrics(Double.MaxValue, Double.MinValue)),
    0
  )
}

case class ClassAttributesMetrics(var attributesMetrics: ArrayBuffer[AttributeMetrics], var count: Int)
case class AttributeMetrics(var mean: Double, var std: Double, var count: Int)
case class AttributeClassesMetrics(var min: Double, var max: Double)
case class Instance(attributes: Array[Double], classLbl: Int)