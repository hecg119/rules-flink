import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import event.Event
import input.{InputConverter, StreamHeader}
import pipes.rul.{PartialRulesProcessor, RulesAggregator}
import utils.IntegerPartitioner

object DistributedRulesJob {

  val outputTag = new OutputTag[Event]("side-output")

  def main(args: Array[String]) {
    println("Starting")

    val numPartitions = 1
    val arffPath = "data\\ELEC_short.arff"
    val extMin = 100
    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
    val eventsStream = rawInputStream.map(new InputConverter(streamHeader))

    val mainStream: DataStream[Event] = eventsStream.iterate((iteration: DataStream[Event]) =>
    {
      val predictionsStream = iteration.process(new RulesAggregator(streamHeader, extMin, outputTag))
      val rulesUpdatesStream = predictionsStream.getSideOutput(outputTag)

      val newConditionsStream = rulesUpdatesStream
        .partitionCustom(new IntegerPartitioner(numPartitions), 0)
        .flatMap(new PartialRulesProcessor(streamHeader, extMin))
        .setParallelism(numPartitions)
        .map(e => e)
        .setParallelism(1)

      newConditionsStream.print()

      (rulesUpdatesStream, predictionsStream) // interleave it more? how?
    }, 1)

    mainStream.print()

    val resultsStream = mainStream.map(new Evaluator())

    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    val result = env.execute("Distributed AMRules")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")

    System.out.println("Execution time: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms")
    println("Accuracy: " + (correct / all))
  }

}
