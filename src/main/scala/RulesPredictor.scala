import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class RulesPredictor extends CheckpointedFunction with FlatMapFunction[String, Double] {

  @transient
  private var rulesState: ListState[String] = _

  private var rules: ListBuffer[String] = ListBuffer[String]()
  private var i: Int = 0

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    rulesState.clear()
    for (element <- rules) {
      rulesState.add(element)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[String](
      "buffered-rules",
      TypeInformation.of(new TypeHint[String]() {})
    )

    rulesState = context.getOperatorStateStore.getListState(descriptor)

    if(context.isRestored) {
      val it = rulesState.get().iterator()
      while (it.hasNext) {
        rules += it.next()
      }
    }
  }

  override def flatMap(t: String, collector: Collector[Double]): Unit = {
    println("getting predictions and updating " + i)
    val res = (1 to 1000).map(x => x * x).distinct
    collector.collect(1.0) // predictions
    rules += "s" // update
    i += 1
  }

}
