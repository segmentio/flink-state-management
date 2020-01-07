package com.segment

import com.segment.message.Sensor
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object SensorManager extends FlatMapFunction[Sensor, Sensor] {
  val LOG = LoggerFactory.getLogger(this.getClass)

  override def flatMap(in: Sensor, out: Collector[Sensor]): Unit = {
    out.collect(in)
  }
}
