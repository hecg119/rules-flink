import java.time.LocalDateTime
import java.util.Properties
import java.util.concurrent.TimeUnit

import eval.Evaluator
import event.{ConditionSchema, Event}
import input.{InputConverter, StreamHeader}
import model.Condition
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import pipes.rul.{PartialRulesProcessor, RulesAggregator}
import utils.{Files, ModuloPartitioner}

object VerticalRulesKafkaJob {

  val metricsUpdateTag = new OutputTag[Event]("metrics-update")

  def main(args: Array[String]) {
    println("Running Vertical Rules (Kafka): " + args.mkString(" "))
    val flinkKafkaConsumer = createConditionConsumer("test", "localhost:2181", "localhost:9092", "test_group")
    val flinkKafkaProducer = createConditionProducer("test", "localhost:9092")

    val arffPath = "data\\ELEC.arff"
    val numPartitions = 8
    val extMin = 100
    val itMaxDelay = 5000

    println(s"Starting VerticalRulesKafkaJob with: $arffPath $numPartitions $extMin $itMaxDelay")

    val streamHeader: StreamHeader = new StreamHeader(arffPath).parse()
    streamHeader.print()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val rawInputStream = env.readTextFile(arffPath).filter(line => !line.startsWith("@") && !line.isEmpty)
    val dataStream = rawInputStream.map(new InputConverter(streamHeader))
    //val controlStream = env.addSource(flinkKafkaConsumer).map(c => s"${c.value} ${c.relation} ${c.attributeIdx}").print()

    // handle dataStream and controlStream by CoMap -> then move to map with RulesAggregator
    // filter controlEvents and broadcast
    // filter dataEvents
    // connect them and move to RulesAggregators

    val predictionsStream = dataStream.process(new RulesAggregator(streamHeader, extMin, metricsUpdateTag)) // setParallelism()
    val rulesUpdatesStream = predictionsStream.getSideOutput(metricsUpdateTag)

    // filter to DefaultRule
    // filter to PartialRulesProcessor (rulesUpdatesStream)

    val newConditionsStream = rulesUpdatesStream
      .map((e: Event) => (e, e.ruleId))
      .partitionCustom(new ModuloPartitioner(numPartitions), 1)
      .flatMap(new PartialRulesProcessor(streamHeader, extMin))
      .setParallelism(numPartitions)
      .map(e => e.condition)
      .addSink(flinkKafkaProducer)

    val resultsStream = predictionsStream.map(new Evaluator())

    //resultsStream.print()
    //resultsStream.countWindowAll(1000, 1000).sum(0).map(s => s / 1000.0).print()

    val result = env.execute("VerticalRulesJob")

    val correct: Double = result.getAccumulatorResult("correct-counter")
    val all: Double = result.getAccumulatorResult("all-counter")
    val accuracy: Double = correct / all
    val time: Long = result.getNetRuntime(TimeUnit.MILLISECONDS) - itMaxDelay

    println(s"Accuracy: $accuracy")
    System.out.println(s"Execution time: $time ms")

    Files.writeResultsToFile("results.csv", arffPath, accuracy, time, "Vertical", numPartitions)
  }

  def createConditionConsumer(topic: String, zookeeperAddress: String, kafkaAddress: String,
                                   kafkaGroup: String): FlinkKafkaConsumer011[Condition] = {
    val props = new Properties()
    props.setProperty("zookeeper.connect", zookeeperAddress)
    props.setProperty("bootstrap.servers", kafkaAddress)
    props.setProperty("group.id", kafkaGroup)
    new FlinkKafkaConsumer011[Condition](topic, new ConditionSchema(), props)
  }

  def createConditionProducer(topic: String, kafkaAddress: String): FlinkKafkaProducer011[Condition] = {
    new FlinkKafkaProducer011[Condition](kafkaAddress, topic, new ConditionSchema())
  }

}
