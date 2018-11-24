import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class StreamHeader(path: String) {

  private var columnsFormat: Array[AttributeFormat] = Array()

  def parse(): StreamHeader = {
    val lines = Source.fromFile(path).getLines
    var done = false
    val formats: ArrayBuffer[AttributeFormat] = ArrayBuffer()

    while (lines.hasNext && !done) {
      val line = lines.next()

      if (line.startsWith("@attribute")) {
        val c = line.split(" ").drop(1)

        if (c(1) == "numeric") {
          formats += AttributeFormat(c(0), numeric=true, Map())
        }
        else if (c(1).startsWith("{")) {
          val values = c(1).replace("{", "").replace("}", "").split(",")
          val mapper: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map()

          for ((v, i) <- values.zipWithIndex) {
            mapper.put(v, i)
          }

          formats += AttributeFormat(c(0), numeric=false, mapper.toMap)
        } else {
          throw new Error("Wrong row format! Aborting the job.")
        }

      } else if (line.startsWith("@data")) done = false
    }

    columnsFormat = formats.toArray
    this
  }

  def attribute(idx: Int, value: String): Double = {
    columnsFormat(idx).mapper(value)
  }

  def print(): Unit = {
    println("Stream header:")
    for (c <- columnsFormat) {
      println(c.name + " " + c.numeric + " {" + c.mapper.mkString(", ") + "}")
    }
  }

}

case class AttributeFormat(name: String, numeric: Boolean, mapper: Map[String, Double])