package pipes.base

import input.{Instance, StreamHeader}
import model.SequentialAMR
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class Predictor(streamHeader: StreamHeader, extMin: Int) extends CheckpointedFunction with MapFunction[Instance, (Double, Double)] {

  private var rulesState: ListState[SequentialAMR] = _
  private val rulesModel: SequentialAMR = new SequentialAMR(streamHeader, extMin)

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def map(instance: Instance): (Double, Double) = {
    //rulesModel.print()
    rulesModel.update(instance)
    (instance.classLbl, rulesModel.predict(instance))
  }

}
