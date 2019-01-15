package event

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import model.Condition
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

class ConditionSchema extends SerializationSchema[Condition] with DeserializationSchema[Condition] {

  override def serialize(condition: Condition): Array[Byte] = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val bytes = mapper.writeValueAsString(condition)
    println(bytes)
    val rev: Condition = mapper.readValue(bytes, classOf[Condition])

    println(s"origin: ${condition.attributeIdx} ${condition.relation} ${condition.value}")
    println(s"rev: ${rev.attributeIdx} ${rev.relation} ${rev.value}")

    bytes.getBytes
  }

  override def isEndOfStream(t: Condition): Boolean = false

  override def deserialize(bytes: Array[Byte]): Condition = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue(bytes, classOf[Condition])
  }

  override def getProducedType: TypeInformation[Condition] = TypeExtractor.getForClass(classOf[Condition])

}
