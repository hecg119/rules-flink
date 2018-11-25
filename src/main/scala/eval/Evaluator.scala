package eval

import org.apache.flink.api.common.functions.MapFunction

class Evaluator extends MapFunction[(Double, Double), Int]{

  override def map(prediction: (Double, Double)): Int = {
    val trueClass = prediction._1.toInt
    val predictedClass = prediction._2.toInt
    if (trueClass == predictedClass) 1 else 0
  }

}