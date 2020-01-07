package com.segment

import com.segment.message.{RawSensorReading, Sensor}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**************************************************************
 * Actual operator
 **************************************************************/
object ReadingFilter {
  val uid = "reading-filter"
  val name = "Reading Filter"

  val sensorDesc = new MapStateDescriptor("ReadingFilter sensors",
    BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Sensor]() {}))

  val broadcastStates: Seq[MapStateDescriptor[_, _]] = Seq(
    sensorDesc
  )
}

class ReadingFilter extends BroadcastProcessFunction[RawSensorReading, Sensor, RawSensorReading] {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def processBroadcastElement(
    sensor: Sensor,
    ctx: BroadcastProcessFunction[RawSensorReading, Sensor, RawSensorReading]#Context,
    out: Collector[RawSensorReading]): Unit = {

    val sensors = ctx.getBroadcastState(ReadingFilter.sensorDesc)
    sensors.put(sensor.id, sensor)
    LOG.info(s"Sensor ${sensor.name} set to enabled = ${sensor.enabled}")
  }

  override def processElement(
    reading: RawSensorReading,
    ctx: BroadcastProcessFunction[RawSensorReading, Sensor, RawSensorReading]#ReadOnlyContext,
    out: Collector[RawSensorReading]): Unit = {

    val sensors = ctx.getBroadcastState(ReadingFilter.sensorDesc)
    val sensor = sensors.get(reading.id)

    if (sensor != null && sensor.enabled) {
      out.collect(reading)
    }
  }
}

/**************************************************************
 * For hydrating the state that the Operator will use after loading from savepoint
 **************************************************************/
class ReadingFilterBootstrapper extends BroadcastStateBootstrapFunction[Tuple2[String, Sensor]] {
  var sensorDesc: MapStateDescriptor[String, Sensor] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    sensorDesc = new MapStateDescriptor("ReadingFilter sensors",
      BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Sensor]() {}))
  }

  override def processElement(value: Tuple2[String, Sensor], ctx: BroadcastStateBootstrapFunction.Context): Unit = {
    val sensors = ctx.getBroadcastState(sensorDesc)
    sensors.put(value.f0, value.f1)
  }
}