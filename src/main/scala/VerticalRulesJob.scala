import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import event.Event
import input.{InputConverter, StreamHeader}
import pipes.rul.{PartialRulesProcessor, RulesAggregator}
import utils.{Files, ModuloPartitioner}

object VerticalRulesJob {

  val metricsUpdateTag = new OutputTag[Event]("metrics-update")

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Too few parameters, expected: 4. Usage: java -jar rules-flink.jar data/ELEC.arff 8 100 5000")
    }

    val arffPath = args(1) //"data\\ELEC_short.arff"
    val numPartitions = args(2).toInt //8
    val extMin = args(3).toInt //100
    val itMaxDelay = args(4).toInt //5000

    println(s"Starting VerticalRulesJob with: $arffPath $numPartitions $extMin $itMaxDelay")

    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
    val eventsStream = rawInputStream.map(new InputConverter(streamHeader))

    val mainStream: DataStream[Event] = eventsStream.iterate((iteration: DataStream[Event]) =>
    {
      val predictionsStream = iteration.process(new RulesAggregator(streamHeader, extMin, metricsUpdateTag))
      val rulesUpdatesStream = predictionsStream.getSideOutput(metricsUpdateTag)

      val newConditionsStream = rulesUpdatesStream
        .map((e: Event) => (e, e.ruleId))
        .partitionCustom(new ModuloPartitioner(numPartitions), 1)
        .flatMap(new PartialRulesProcessor(streamHeader, extMin))
        .setParallelism(numPartitions)
        .map(e => e)

      (newConditionsStream, predictionsStream)
    }, itMaxDelay)

    val resultsStream = mainStream.map(new Evaluator())

    //resultsStream.print()
    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    val result = env.execute("VerticalRulesJob")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")
    val accuracy: Double = correct / all
    val time: Long = result.getNetRuntime(TimeUnit.MILLISECONDS) - itMaxDelay

    println(s"Accuracy: $accuracy")
    System.out.println(s"Execution time: $time ms")

    Files.writeResultsToFile("results.csv", arffPath, accuracy, time, "Vertical", numPartitions)
  }

}