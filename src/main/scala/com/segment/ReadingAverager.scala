package com.segment

import com.segment.message.{RawSensorReading, Sensor, SensorReading}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.{KeyedStateBootstrapFunction, KeyedStateReaderFunction}
import org.apache.flink.state.api.functions.KeyedStateReaderFunction.Context
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

/**************************************************************
 * Actual operator
 **************************************************************/
object ReadingAverager {
  val uid = "reading-averager"
  val name = "Reading Averager"

  // State descriptors
  val stateDesc = new ValueStateDescriptor("ReadingAverager valueState", BasicTypeInfo.DOUBLE_TYPE_INFO)
  val sensorMap = new MapStateDescriptor("ReadingAverager sensors",
    BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Sensor]() {}))

  val broadcastStates = Seq(sensorMap)
}

class ReadingAverager extends KeyedBroadcastProcessFunction[String, RawSensorReading, Sensor, SensorReading] {
  override def processBroadcastElement(
    value: Sensor,
    ctx: KeyedBroadcastProcessFunction[String, RawSensorReading, Sensor, SensorReading]#Context,
    out: Collector[SensorReading]): Unit = {

    ctx.getBroadcastState(ReadingAverager.sensorMap).put(value.id, value)
  }

  override def processElement(
    reading: RawSensorReading,
    ctx: KeyedBroadcastProcessFunction[String, RawSensorReading, Sensor, SensorReading]#ReadOnlyContext,
    out: Collector[SensorReading]): Unit = {

    val sensorMap = ctx.getBroadcastState(ReadingAverager.sensorMap)
    if (!sensorMap.contains(reading.id)) {
      return
    }

    val sensor = sensorMap.get(reading.id)
    val value = getRuntimeContext.getState(ReadingAverager.stateDesc)

    if (value.value() == null) {
      value.update(reading.value)
    } else {
      value.update((reading.value + value.value()) / 2)
    }

    // emit a SensorReading with the average temperature
    out.collect(SensorReading(sensor.id, reading.timestamp, value.value(), sensor.unit, sensor.name))
  }
}

/**************************************************************
 * For retrieving state from the savepoint
 **************************************************************/
case class KeyedState(key: String, value: Double)

class ReadingAveragerStateFunction extends KeyedStateReaderFunction[String, KeyedState] {
   var state: ValueState[java.lang.Double] = _

  override def open(parameters: Configuration) {
    state = getRuntimeContext.getState(ReadingAverager.stateDesc)
  }

  override def readKey(
     key: String,
     ctx: Context,
     out: Collector[KeyedState]) : Unit = {

    if (SensorService.sensorEnabled(key)) {
      out.collect(KeyedState(key, state.value()));
    }
  }
}

/**************************************************************
 * For hydrating the state that the Operator will use after loading from savepoint
 **************************************************************/
class ReadingAveragerBootstrapFunction extends KeyedStateBootstrapFunction[String, KeyedState] {
  var state: ValueState[java.lang.Double] = _

  override def open(parameters: Configuration) {
    state = getRuntimeContext.getState(ReadingAverager.stateDesc)
  }

  override def processElement(reading: KeyedState, ctx: KeyedStateBootstrapFunction[String, KeyedState]#Context): Unit = {
    state.update(reading.value)
  }
}