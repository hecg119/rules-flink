import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import input.{InputConverter, StreamHeader}
import pipes.base.SinglePredictor
import utils.Files

object SequentialClassifierJob {

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Too few parameters, expected: 3. Usage: java -jar rules-flink.jar data/ELEC.arff 8 100")
    }

    val arffPath = args(1) //"data\\ELEC.arff"
    val numPartitions = args(2).toInt //8
    val extMin = args(3).toInt //100

    println(s"Starting SequentialClassifierJob with: $arffPath $numPartitions $extMin")

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

    val result = env.execute("Sequential AMRules")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")
    val accuracy: Double = correct / all
    val time: Long = result.getNetRuntime(TimeUnit.MILLISECONDS)

    println(s"Accuracy: $accuracy")
    System.out.println(s"Execution time: $time ms")

    Files.writeResultsToFile("tmp", arffPath, accuracy, time, "Horizontal", numPartitions)
  }

}
