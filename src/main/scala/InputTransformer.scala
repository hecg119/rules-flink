import org.apache.flink.api.common.functions.MapFunction

class InputTransformer extends MapFunction[String, Instance] {

  override def map(rawInput: String): Instance = {
    val columns = rawInput.split(",")
    val features = columns.dropRight(1).map(col => col.toDouble)
    Instance(features, columns.last.toInt)
  }

}
