package input

import org.apache.flink.api.common.functions.MapFunction

class InputConverter(streamHeader: StreamHeader) extends MapFunction[String, Instance] {

  override def map(rawInput: String): Instance = {
    val columns = rawInput.split(",")

    val convertedColumns = columns
      .zipWithIndex
      .map({ case (c: String, idx: Int) => streamHeader.column(idx, c) })

    Instance(convertedColumns.dropRight(1), convertedColumns.last)
  }

}

case class Instance(attributes: Array[Double], classLbl: Double)
