package input

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class StreamHeader(path: String) extends Serializable {

  private var columnsFormat: Array[ColumnFormat] = Array()
  private var _attrNum: Int = 0
  private var _clsNum: Int = 0

  def parse(): StreamHeader = {
    val lines = Source.fromFile(path).getLines
    var done = false
    val formats: ArrayBuffer[ColumnFormat] = ArrayBuffer()

    while (lines.hasNext && !done) {
      val line = lines.next()

      if (line.toLowerCase.startsWith("@attribute")) {
        val c = line.split(" ").drop(1)

        if (c(1).toLowerCase == "numeric") {
          formats += ColumnFormat(c(0), numeric=true, Map())
        }
        else if (c(1).startsWith("{")) {
          val values = c(1).replace("{", "").replace("}", "").replaceAll("\"", "").replaceAll("'", "").trim.split(",")
          val mapper: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map()

          for ((v, i) <- values.zipWithIndex) {
            mapper.put(v, i)
          }

          formats += ColumnFormat(c(0), numeric=false, mapper.toMap)
        } else {
          throw new Error("Wrong row format! Aborting the job.")
        }

      } else if (line.toLowerCase.startsWith("@data")) done = false
    }

    _attrNum = formats.length - 1
    _clsNum = formats.last.mapper.size
    columnsFormat = formats.toArray
    this
  }

  def column(idx: Int, value: String): Double = {
    if (columnsFormat(idx).numeric) value.toDouble else columnsFormat(idx).mapper(value)
  }

  def columnName(idx: Int): String = {
    columnsFormat(idx).name
  }

  def attrNum(): Int = _attrNum

  def clsNum(): Int = _clsNum

  def print(): Unit = {
    println("Stream header:")
    println("\t#attributes = " + _attrNum + "\n\t#classes = " + _clsNum)
    for (c <- columnsFormat) {
      println("\t" + c.name + " " + c.numeric + " {" + c.mapper.mkString(", ") + "}")
    }
  }

}

case class ColumnFormat(name: String, numeric: Boolean, mapper: Map[String, Double])