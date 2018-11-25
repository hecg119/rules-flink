package eval

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.accumulators.{DoubleCounter, IntCounter}
import org.apache.flink.configuration.Configuration

class Evaluator extends RichMapFunction[(Double, Double), Int]{

  private val allCounter = new DoubleCounter
  private val correctCounter = new DoubleCounter

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.addAccumulator("all-counter", allCounter)
    getRuntimeContext.addAccumulator("correct-counter", correctCounter)
  }

  override def map(prediction: (Double, Double)): Int = {
    val trueClass = prediction._1.toInt
    val predictedClass = prediction._2.toInt
    val correct = if (trueClass == predictedClass) 1 else 0

    allCounter.add(1.0)
    correctCounter.add(correct)

    correct
  }

}