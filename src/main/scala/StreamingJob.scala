import org.apache.flink.streaming.api.scala._
import java.util.concurrent.TimeUnit


object StreamingJob {

  def main(args: Array[String]) {
    println("Starting")
    val arffPath = "data\\ELEC.arff"
    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//
//    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
//    val instancesStream = rawInputStream.map(new InputConverter(streamHeader))
//    val predictionsStream = instancesStream.map(new RulesPredictor())
//    val resultsStream = predictionsStream.map(new Evaluator())
//
//    //resultsStream.countWindowAll(1000, 1).sum(0).print()
//
//    // distribute each instance to rule learners
//    // split it into smaller operators? so we calculate rules statistics / split metrics for attributes in parallel (collector)
//
//    val result = env.execute("Sequential AMRules")
//    System.out.println("The job took " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms to execute")
  }

}
