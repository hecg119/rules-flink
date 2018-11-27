import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import input.{InputConverter, StreamHeader}
import pipes.base.Predictor

object SequentialClassifierJob {

  def main(args: Array[String]) {
    println("Starting")
    val numPartitions = 4
    val arffPath = "data\\ELEC.arff"
    val extMin = 100
    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
    val instancesStream = rawInputStream.map(new InputConverter(streamHeader))
    val predictionsStream = instancesStream.map(new Predictor(streamHeader, extMin)).setParallelism(numPartitions)
    val resultsStream = predictionsStream.map(new Evaluator())

    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    val result = env.execute("Sequential AMRules")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")

    System.out.println("Execution time: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms")
    println("Accuracy: " + (correct / all))
  }

}
