package utils

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

class SimpleMerge[T] extends CoProcessFunction[T, T, T] {

  override def processElement1(event: T, ctx: CoProcessFunction[T, T, T]#Context, out: Collector[T]): Unit = {
    out.collect(event)
  }

  override def processElement2(event: T, ctx: CoProcessFunction[T, T, T]#Context, out: Collector[T]): Unit = {
    out.collect(event)
  }

}
