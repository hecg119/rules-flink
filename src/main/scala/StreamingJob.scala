import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.distribution.NormalDistribution

import scala.collection.mutable.ArrayBuffer

object StreamingJob {

  def main(args: Array[String]) {
    println("Starting")

    val p1 = new NormalDistribution(0, 1).cumulativeProbability(0)
    val p2 = new NormalDistribution(5, 1).cumulativeProbability(5)
    println(p1, p2)

//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //env.enableCheckpointing(1000)
//
//    val text = env.readTextFile("data\\ELEC.arff").filter(line => !line.startsWith("@") && !line.isEmpty).setParallelism(1)
//    text.flatMap(new RulesPredictor()).setParallelism(1)
//
//    // distribute each instance to rule learners
//
//    // split it into smaller operators? so we calculate rules statistics / split metrics for attributes in parallel (collector)
//    // classification results / measurements as an output
//    // todo: poc, sequential algorithm and parallel version
//
//    val result = env.execute("File Window WordCount")
//    System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms to execute")
  }
}
