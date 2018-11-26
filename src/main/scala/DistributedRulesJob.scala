import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import input.{InputConverter, Instance, StreamHeader}
import model.pipes.ens.{IntegerPartitioner, ReplicateInstance}
import model.pipes.rul.{Event, PartialRulesProcessor, RulesAggregator}
import model.pipes.seq.Predictor

object DistributedRulesJob {

  val outputTag = new OutputTag[Event]("side-output")

  def main(args: Array[String]) {
    println("Starting")
    val numPartitions = 8
    val arffPath = "data\\ELEC_short.arff"
    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input: DataStream[Event] = env.fromCollection(Array.fill(100)(Event("Instance")))

    val mainStream: DataStream[Event] = input.iterate((iteration: DataStream[Event]) =>
    {
      val predictionsStream = iteration.process(new RulesAggregator(outputTag))
      val rulesUpdatesStream = predictionsStream.getSideOutput(outputTag)
      val newConditionsStream = rulesUpdatesStream.flatMap(new PartialRulesProcessor())
      newConditionsStream.print()
      (newConditionsStream, predictionsStream)
    }, 1)

    mainStream.print()

//    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
//    val instancesStream = rawInputStream.map(new InputConverter(streamHeader))
//    val predictionsStream = instancesStream.map(new Predictor(streamHeader))
//    val resultsStream = predictionsStream.map(new Evaluator())

    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    //    val partialPredictions = instancesStream
    //      .flatMap(new ReplicateInstance(numPartitions))
    //      .partitionCustom(new IntegerPartitioner(numPartitions), 0)
    //      .map(new Predictor(ensemble=true))
    //      .setParallelism(numPartitions)

    val result = env.execute("Sequential AMRules")

//    val correct: Double = result.getAccumulatorResult("correct-counter")
//    val all: Double = result.getAccumulatorResult("all-counter")
//
    System.out.println("Execution time: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms")
//    println("Accuracy: " + (correct / all))
  }

}
