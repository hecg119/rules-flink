package pipes.base

import input.{StreamHeader}
import model.SequentialAMR
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import event.Event

class Predictor(streamHeader: StreamHeader, extMin: Int) extends CheckpointedFunction with MapFunction[Event, Event] {

  private var rulesState: ListState[SequentialAMR] = _
  private val rulesModel: SequentialAMR = new SequentialAMR(streamHeader, extMin)

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {}

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def map(instanceEvent: Event): Event = {
    if (!instanceEvent.getType.equals("Instance")) {
      throw new Error(s"This operator handles only Instance events. Received: ${instanceEvent.getType}")
    }

    val instance = instanceEvent.instance

    //rulesModel.print()
    rulesModel.update(instance)
    new Event("Prediction", instance.classLbl, rulesModel.predict(instance))
  }

}
