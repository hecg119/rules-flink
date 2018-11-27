package utils

import event.Event
import org.apache.flink.api.common.functions.Partitioner

class IntegerPartitioner(numPartitions: Int) extends Partitioner[Event] {

  override def partition(updateRuleEvent: Event, key: Int): Int = {
    if (!updateRuleEvent.getType.equals("UpdateRule")) {
      throw new Error(s"This operator handles only UpdateRule events. Received: ${updateRuleEvent.getType}")
    }
    updateRuleEvent.ruleId % numPartitions
  }

}
