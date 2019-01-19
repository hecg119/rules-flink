package model

import input.Instance
import org.apache.commons.math3.distribution.NormalDistribution

import scala.collection.mutable.ArrayBuffer

class RuleMetrics(attrNum: Int, clsNum: Int, var classesAttributesMetrics: ArrayBuffer[ClassAttributesMetrics],
                  var attributesClassesMetrics: ArrayBuffer[AttributeClassesMetrics], var count: Int) extends Serializable {

  private val DELTA: Double = 1 - 0.95
  private val R: Double = 1.0
  private val SPLIT_GRAN: Double = 0.2

  def this(attrNum: Int, clsNum: Int) {
    this(
      attrNum,
      clsNum,
      ArrayBuffer.fill(clsNum)(ClassAttributesMetrics(ArrayBuffer.fill(attrNum)(AttributeMetrics(0, 0)), 0)),
      ArrayBuffer.fill(attrNum)(AttributeClassesMetrics(Double.MaxValue, Double.MinValue)),
      0
    )
  }

  def updateStatistics(instance: Instance): Unit = {
    val clsIdx = instance.classLbl.toInt
    val classAttributesMetrics = classesAttributesMetrics(clsIdx)

    count = count + 1
    classAttributesMetrics.count = classAttributesMetrics.count + 1

    for ((attVal, i) <- instance.attributes.zipWithIndex) { // todo: distinguish numeric/nominal
      val attributeClassesMetrics = attributesClassesMetrics(i)
      if (attVal > attributeClassesMetrics.max) attributeClassesMetrics.max = attVal // todo: get m - 3std / m + 3std instead, against outliers
      if (attVal < attributeClassesMetrics.min) attributeClassesMetrics.min = attVal
      attributesClassesMetrics(i) = attributeClassesMetrics

      val classAttributeMetrics = classAttributesMetrics.attributesMetrics(i)

      if (classAttributesMetrics.count < 2) {
        classAttributeMetrics.mean = attVal
        classAttributeMetrics.std = 0.0
      } else {
        val lastMean = classAttributeMetrics.mean
        val lastStd = classAttributeMetrics.std
        val count = classAttributesMetrics.count

        classAttributeMetrics.mean = classAttributeMetrics.mean + ((attVal - lastMean) / count)
        classAttributeMetrics.std = math.sqrt(
          ((count - 2.0) / (count - 1.0)) * math.pow(lastStd, 2) + (1.0 / count) * math.pow(attVal - lastMean, 2)
        )
      }

      classAttributesMetrics.attributesMetrics(i) = classAttributeMetrics
    }

    classesAttributesMetrics(clsIdx) = classAttributesMetrics // todo: update error/remove rule + classPrediction
  }

  def expandRule(): (Condition, Double) = {
    val ruleEntropy = entropy(classesAttributesMetrics.map(cm => cm.count.toDouble / count).toList)
    val bound = calcHoeffdingBound(count)
    var expansion: (Condition, Double) = null

    var bestSplit: (Condition, Double, Double) = (Condition(-1, "", -1), Double.MaxValue, -1.0)

    if (ruleEntropy > bound) {
      (0 until attrNum).foreach(attrIdx => {
        val attrBestSplit = findBestSplit(attrIdx)

        if (attrBestSplit._2 < bestSplit._2) {
          bestSplit = attrBestSplit
        }
      })

      if (ruleEntropy - bestSplit._2 > bound) {
        expansion = (bestSplit._1, bestSplit._3.toDouble)
        releaseStatistics()
      }
    }

    expansion
  }

  private def findBestSplit(attIdx: Int): (Condition, Double, Double) = { // todo: distinguish numeric/nominal
    val min = attributesClassesMetrics(attIdx).min
    val max = attributesClassesMetrics(attIdx).max
    val step = (max - min) * SPLIT_GRAN

    var minEntropySplit: (Double, Double, Int) = (Double.MaxValue, 0.0, -1)
    var splitVal = min + step

    while (splitVal < max) {
      val psl: ArrayBuffer[Double] = ArrayBuffer()
      val psr: ArrayBuffer[Double] = ArrayBuffer()
      var spl = Double.MinPositiveValue
      var spr = Double.MinPositiveValue
      var maxCls = (Double.MinValue, -1)

      (0 until clsNum).foreach((clsIdx) => {
        val mean = classesAttributesMetrics(clsIdx).attributesMetrics(attIdx).mean
        val std = classesAttributesMetrics(clsIdx).attributesMetrics(attIdx).std + Double.MinPositiveValue
        val p = new NormalDistribution(mean, std).cumulativeProbability(splitVal) + Double.MinPositiveValue
        val cp = classesAttributesMetrics(clsIdx).count.toDouble / count

        psl += p * cp
        spl = spl + p * cp
        psr += (1 - p) * cp
        spr = spr + (1 - p) * cp

        if (psl.last > maxCls._1) maxCls = (psl.last, psl.length - 1)
      })

      val splitEntropy = (entropy(psl.map(p => p / spl).toList) + entropy(psr.map(p => p / spr).toList)) / 2.0

      if (splitEntropy < minEntropySplit._1) minEntropySplit = (splitEntropy, splitVal, maxCls._2)

      splitVal = splitVal + step
    }

    (Condition(attIdx, "<=", minEntropySplit._2), minEntropySplit._1, minEntropySplit._3.toDouble) // todo: left or right, depending on entropy
  }

  private def releaseStatistics(): Unit = {
    classesAttributesMetrics = ArrayBuffer.fill(clsNum)(ClassAttributesMetrics(ArrayBuffer.fill(attrNum)(AttributeMetrics(0, 0)), 0))
    attributesClassesMetrics = ArrayBuffer.fill(attrNum)(AttributeClassesMetrics(Double.MaxValue, Double.MinValue))
    count = 0
  }

  private def entropy(ps: List[Double]): Double = {
    ps.map(p => if (p == 0) 0 else -p * math.log10(p) / math.log10(2.0)).sum
  }

  private def calcHoeffdingBound(n: Int): Double = math.sqrt(R * R * math.log(1 / DELTA) / (2 * n))

}

case class ClassAttributesMetrics(var attributesMetrics: ArrayBuffer[AttributeMetrics], var count: Int)
case class AttributeMetrics(var mean: Double, var std: Double)
case class AttributeClassesMetrics(var min: Double, var max: Double)