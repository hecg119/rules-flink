import java.io.File

import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit
import java.nio.file.Paths

import eval.Evaluator
import input.{InputConverter, StreamHeader}
import pipes.base.SinglePredictor
import utils.Files

object HorizontalRulesJob {

  def main(args: Array[String]) {
    println("Running Horizontal Rules: " + args.mkString(" "))

//    if (args.length < 3) {
//      println("Too few parameters, expected: 3. Usage: java -jar horizontal.jar data/ELEC.arff 8 100")
//      System.exit(1)
//    }
//
//    val arffPath = s"${Paths.get(".").toAbsolutePath}/${args(0)}" //"data\\ELEC_short.arff"
//    val numPartitions = args(1).toInt //8
//    val extMin = args(2).toInt //100

    val arffPath = "data\\ELEC.arff"
    val numPartitions = 8
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
