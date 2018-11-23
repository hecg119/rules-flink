import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

object StreamingJob {

  def main(args: Array[String]) {
    println("Starting")

    var x = ArrayBuffer(1,2,3)
    var xx = x(0)
    xx = 666
    x(0) = xx
    println(x)

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
