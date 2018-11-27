package input

import event.Event
import org.apache.flink.api.common.functions.MapFunction

class InputConverter(streamHeader: StreamHeader) extends MapFunction[String, Event] {

  override def map(rawInput: String): Event = {
    val columns = rawInput.split(",")

    val convertedColumns = columns
      .zipWithIndex
      .map({ case (c: String, idx: Int) => streamHeader.column(idx, c) })

    new Event("Instance", Instance(convertedColumns.dropRight(1), convertedColumns.last))
  }

}

case class Instance(attributes: Array[Double], classLbl: Double)
