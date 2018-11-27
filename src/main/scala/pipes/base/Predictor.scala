package pipes.base

import input.StreamHeader
import model.SequentialAMR
import org.apache.flink.api.common.functions.MapFunction
import event.Event

class Predictor(streamHeader: StreamHeader, extMin: Int) extends MapFunction[Event, Event] {

  private val rulesModel: SequentialAMR = new SequentialAMR(streamHeader, extMin) // todo: add state management

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
