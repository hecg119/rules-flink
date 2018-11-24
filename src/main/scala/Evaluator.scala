import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.util.Collector

class Evaluator extends MapFunction[(Int, Int), Double]{

  override def map(prediction: (Int, Int)): Int = {
    val trueClass = prediction._1
    val predictedClass = prediction._2
    if (trueClass == predictedClass) 1 else 0
  }
}
