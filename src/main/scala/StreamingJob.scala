import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.distribution.NormalDistribution

import scala.collection.mutable.ArrayBuffer

object StreamingJob {

  def main(args: Array[String]) {
    println("Starting")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile("data\\ELEC.arff").filter(line => !line.startsWith("@") && !line.isEmpty)
    val instancesStream = rawInputStream.map(new InputTransformer())
    val predictionsStream = instancesStream.map(new RulesPredictor())
    val resultsStream = predictionsStream.map(new Evaluator())

    resultsStream.countWindowAll(1000, 1).sum(0)

    // distribute each instance to rule learners
    // split it into smaller operators? so we calculate rules statistics / split metrics for attributes in parallel (collector)

    val result = env.execute("Distributed AMRules")
    System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms to execute")
  }
}
