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
    val arffPath = "data\\ELEC.arff"
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

      val newConditionsStream1 = rulesUpdatesStream
        .map((e: Event) => (e, e.ruleId))
        .partitionCustom(new IntegerPartitioner(numPartitions), 1)
        .flatMap(new PartialRulesProcessor(streamHeader, extMin))
        .setParallelism(numPartitions)

      //newConditionsStream1.print()

      val newConditionsStream2 = newConditionsStream1.map(e => e)
        .setParallelism(1)

      (newConditionsStream1, predictionsStream)
    }, 5000)

    val resultsStream = mainStream.map(new Evaluator())

    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    val result = env.execute("Distributed AMRules")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")

    System.out.println("Execution time: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms")
    println("Accuracy: " + (correct / all))
  }

}
