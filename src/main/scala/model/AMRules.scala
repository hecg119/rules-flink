package model

import input.{Instance, StreamHeader}
import org.apache.commons.math3.distribution.NormalDistribution

import scala.collection.mutable.ArrayBuffer

class AMRules(streamHeader: StreamHeader) extends Serializable {

  private val attrNum: Int = streamHeader.attrNum()
  private val clsNum: Int = streamHeader.clsNum()

  private val rules: ArrayBuffer[Rule] = ArrayBuffer(new Rule())
  private val rulesStats: ArrayBuffer[RuleStatistics] = ArrayBuffer(new RuleStatistics(attrNum, clsNum))

  private val EXT_MIN: Int = 1000
  private val DELTA: Double = 1 - 0.95
  private val R: Double = 1.0
  private val SPLIT_GRAN: Double = 0.2

  def update(instance: Instance): Unit = {
    var covered = false

    for ((rule, ruleId) <- rules.zipWithIndex) { // dist
      if (ruleId > 0 && isCovered(instance, rule)) {
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
        rules.append(rules(0).copy())
        rulesStats.append(rulesStats(0).copy())
        rules(0) = new Rule()
        rulesStats(0) = new RuleStatistics(attrNum, clsNum)
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
    val clsIdx = instance.classLbl.toInt
    val classAttributesMetrics = ruleStats.classesAttributesMetrics(clsIdx)

    ruleStats.count = ruleStats.count + 1
    classAttributesMetrics.count = classAttributesMetrics.count + 1

    for ((attVal, i) <- instance.attributes.zipWithIndex) { // dist? + todo: distinguish numeric/nominal
      val attributeClassesMetrics = ruleStats.attributesClassesMetrics(i)
      if (attVal > attributeClassesMetrics.max) attributeClassesMetrics.max = attVal // todo: get m - 3std / m + 3std instead, against outliers
      if (attVal < attributeClassesMetrics.min) attributeClassesMetrics.min = attVal
      ruleStats.attributesClassesMetrics(i) = attributeClassesMetrics

      val classAttributeMetrics = classAttributesMetrics.attributesMetrics(i)

      if (classAttributesMetrics.count < 2) {
        classAttributeMetrics.mean = attVal
        classAttributeMetrics.std = 0
      } else {
        val lastMean = classAttributeMetrics.mean
        val lastStd = classAttributeMetrics.std
        val count = classAttributesMetrics.count // todo: check

        classAttributeMetrics.mean = classAttributeMetrics.mean + ((attVal - lastMean) / count)
        classAttributeMetrics.std = ((count - 2.0) / (count - 1.0)) * math.pow(lastStd, 2) + (1.0 / count) * math.pow(attVal - lastMean, 2)
      }

      classAttributesMetrics.attributesMetrics(i) = classAttributeMetrics // todo: use windowed stats
    }

    ruleStats.classesAttributesMetrics(instance.classLbl.toInt) = classAttributesMetrics
    rulesStats(ruleId) = ruleStats // todo: update error
  }

  private def expandRule(ruleId: Int): Unit = {
    val ruleEntropy = entropy(rulesStats(ruleId).classesAttributesMetrics.map(cm => cm.count.toDouble / rulesStats(ruleId).count).toList)
    val bound = calcHoeffdingBound(rulesStats(ruleId).count)

    var bestSplit: (Condition, Double) = (Condition(-1, "", -1), Double.MaxValue)

    if (ruleEntropy > bound) {
      (0 until attrNum).foreach(attrIdx => { // dist?
        val attrBestSplit = findBestSplit(ruleId, attrIdx) // dist?

        if (attrBestSplit._2 < bestSplit._2) {
          bestSplit = attrBestSplit
        }
      })

      if (ruleEntropy - bestSplit._2 > bound) {
        rules(ruleId).conditions.append(bestSplit._1)
        releaseStatistics(ruleId) // todo: really? then this rule classifies at random
      }
    }
  }

  private def releaseStatistics(ruleId: Int): Unit = {
    rulesStats(ruleId) = new RuleStatistics(attrNum, clsNum) // todo: ok?
  }

  private def entropy(ps: List[Double]): Double = {
    ps.map(p => if (p == 0) 0 else -p * math.log10(p) / math.log10(2.0)).sum
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
      val psl: ArrayBuffer[Double] = ArrayBuffer()
      val psr: ArrayBuffer[Double] = ArrayBuffer()
      var spl = Double.MinPositiveValue
      var spr = Double.MinPositiveValue

      (0 until clsNum).foreach((clsIdx) => {
        val mean = classesAttributeMetrics(clsIdx).attributesMetrics(attIdx).mean
        val std = classesAttributeMetrics(clsIdx).attributesMetrics(attIdx).std + Double.MinPositiveValue
        val p = new NormalDistribution(mean, std).cumulativeProbability(splitVal)
        val cp = classesAttributeMetrics(clsIdx).count.toDouble / rulesStats(ruleId).count

        psl += p * cp
        spl = spl + p * cp
        psr += (1 - p) * cp
        spr = spr + (1 - p) * cp
      })

      val splitEntropy = entropy(psl.map(p => p / spl).toList) + entropy(psr.map(p => p / spr).toList)

      if (splitEntropy < minEntropySplit._1) minEntropySplit = (splitEntropy, splitVal)

      splitVal = splitVal + step
    }

    (Condition(attIdx, "<=", minEntropySplit._2), minEntropySplit._1) // todo: pick best operator based on the number of samples (wight left/right)
  }

  def predict(instance: Instance): Double = {
    val votes = ArrayBuffer.fill(clsNum)(0)

    for ((rule, ruleId) <- rules.zipWithIndex) {
      if (ruleId > 0 && isCovered(instance, rule)) {
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

  def print(): Unit = {
    println(s"\nCurrent AMRules [${rules.length - 1}]:")

    for ((rule, ruleId) <- rules.drop(1).zipWithIndex) {
      var cstr: ArrayBuffer[String] = ArrayBuffer()

      for (c <- rule.conditions) {
        cstr += s"${streamHeader.columnName(c.attributeIdx)} ${c.relation} ${c.value}"
      }

      println(s"Rule $ruleId: ${cstr.mkString(" AND ")}")
    }
    println
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
    ArrayBuffer.fill(clsNum)(ClassAttributesMetrics(ArrayBuffer.fill(attrNum)(AttributeMetrics(0, 0)), 0)),
    ArrayBuffer.fill(attrNum)(AttributeClassesMetrics(Double.MaxValue, Double.MinValue)),
    0
  )
}

case class ClassAttributesMetrics(var attributesMetrics: ArrayBuffer[AttributeMetrics], var count: Int)
case class AttributeMetrics(var mean: Double, var std: Double)
case class AttributeClassesMetrics(var min: Double, var max: Double)