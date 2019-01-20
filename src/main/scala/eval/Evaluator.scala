package eval

import event.{Event, PredictionEvent}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.accumulators.DoubleCounter
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
    predictionEvent match {
      case e: PredictionEvent =>
        val prediction = e.prediction.toInt
        val trueClass = e.trueClass.toInt
        val correct = if (trueClass == prediction) 1 else 0

        allCounter.add(1.0)
        correctCounter.add(correct)

        correct
      case _ => throw new Error(s"This operator handles only PredictionEvent. Received: ${predictionEvent.getClass}")
    }
  }

}