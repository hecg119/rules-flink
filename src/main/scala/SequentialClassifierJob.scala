import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import eval.Evaluator
import input.{InputConverter, Instance, StreamHeader}
import model.pipes.ens.{IntegerPartitioner, ReplicateInstance}
import model.pipes.seq.Predictor

object SequentialClassifierJob {

  def main(args: Array[String]) {
    println("Starting")
    val numPartitions = 8
    val arffPath = "data\\ELEC.arff"
    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
    val instancesStream = rawInputStream.map(new InputConverter(streamHeader))
    val predictionsStream = instancesStream.map(new Predictor(streamHeader))
    val resultsStream = predictionsStream.map(new Evaluator())

    //resultsStream.countWindowAll(1000, 1).sum(0).print()

//    val partialPredictions = instancesStream
//      .flatMap(new ReplicateInstance(numPartitions))
//      .partitionCustom(new IntegerPartitioner(numPartitions), 0)
//      .map(new Predictor(ensemble=true))
//      .setParallelism(numPartitions)

    val result = env.execute("Sequential AMRules")
    System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms to execute")
  }

}
