package model

import input.Instance
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction


class RulesPredictor extends CheckpointedFunction with MapFunction[Instance, (Int, Int)] {

  private var rulesState: ListState[AMRules] = _
  private val rulesModel: AMRules = new AMRules()
  private var i: Int = 0

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def map(instance: Instance): (Int, Int) = {
//    rulesModel.update(instance)
//    (instance.classLbl, rulesModel.predict(instance))
    (0,0)
  }

}
