package eval

import event.Event
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.accumulators.{DoubleCounter, IntCounter}
import org.apache.flink.configuration.Configuration

class Evaluator extends RichMapFunction[Event, Int]{

  private val allCounter = new DoubleCounter
  private val correctCounter = new DoubleCounter

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.addAccumulator("all-counter", allCounter)
    getRuntimeContext.addAccumulator("correct-counter", correctCounter)
  }

  override def map(predictionEvent: Event): Int = {
    if (!predictionEvent.getType.equals("Prediction")) {
      throw new Error(s"This operator handles only Prediction events. Received: ${predictionEvent.getType}")
    }

    val prediction = predictionEvent.prediction.toInt
    val trueClass = predictionEvent.trueClass.toInt
    val correct = if (trueClass == prediction) 1 else 0

    allCounter.add(1.0)
    correctCounter.add(correct)

    correct
  }

}