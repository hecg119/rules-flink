package pipes.base

import input.StreamHeader
import model.SequentialAMR
import org.apache.flink.api.common.functions.MapFunction
import event.{Event, InstanceEvent, PredictionEvent}

class SinglePredictor(streamHeader: StreamHeader, extMin: Int) extends MapFunction[Event, Event] {

  private val rulesModel: SequentialAMR = new SequentialAMR(streamHeader, extMin) // todo: add state management

  override def map(instanceEvent: Event): Event = {
    instanceEvent match {
      case e: InstanceEvent =>
        val instance = e.instance
        rulesModel.update(instance)
        PredictionEvent(instance.classLbl, rulesModel.predict(instance))
      case _ => throw new Error(s"This operator handles only InstanceEvent. Received: ${instanceEvent.getClass}")
    }
  }

}
