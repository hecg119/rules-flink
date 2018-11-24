package eval

import org.apache.flink.api.common.functions.MapFunction

class Evaluator extends MapFunction[(Int, Int), Int]{

  override def map(prediction: (Int, Int)): Int = {
    val trueClass = prediction._1
    val predictedClass = prediction._2
    if (trueClass == predictedClass) 1 else 0
  }

}
