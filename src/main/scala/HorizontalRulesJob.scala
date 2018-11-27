import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import input.{InputConverter, StreamHeader}
import pipes.base.SinglePredictor
import utils.Files

object HorizontalRulesJob {

  def main(args: Array[String]) {
//    if (args.length < 3) {
//      println("Too few parameters, expected: 3. Usage: java -jar rules-flink.jar data/ELEC.arff 8 100")
//    }

    val arffPath = "data\\WEATHER.arff"
    val numPartitions = 1
    val extMin = 100

    println(s"Starting HorizontalRulesJob with: $arffPath $numPartitions $extMin")

    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
    val instancesStream = rawInputStream.map(new InputConverter(streamHeader))
    val predictionsStream = instancesStream.map(new SinglePredictor(streamHeader, extMin)).setParallelism(numPartitions)
    val resultsStream = predictionsStream.map(new Evaluator())

    //resultsStream.print()
    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    val result = env.execute("HorizontalRulesJob")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")
    val accuracy: Double = correct / all
    val time: Long = result.getNetRuntime(TimeUnit.MILLISECONDS)

    println(s"Accuracy: $accuracy")
    System.out.println(s"Execution time: $time ms")

    Files.writeResultsToFile("results.csv", arffPath, accuracy, time, "Horizontal", numPartitions)
  }

}
