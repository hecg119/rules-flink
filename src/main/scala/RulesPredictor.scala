import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class RulesPredictor extends CheckpointedFunction with MapFunction[Instance, (Int, Int)] {

  @transient
  private var rulesState: ListState[String] = _
  private val rulesModel: AMRules = new AMRules()
  private var i: Int = 0

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def map(instance: Instance): (Int, Int) = {
    rulesModel.update(instance)
    (instance.classLbl, rulesModel.predict(instance))
  }
}
