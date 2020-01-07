package com.segment.message

import java.io.ByteArrayInputStream

import com.segment.json.InstantSerializer
import org.json4s.DefaultFormats
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SerializationSchema}
import org.json4s.native.JsonMethods
import org.slf4j.LoggerFactory
import org.json4s.native.Serialization.write

/**
 * SerializationSchema describes how to serialize a ComputedValue
 */
class SensorReadingSerializationSchema extends SerializationSchema[SensorReading] {

  private val LOG = LoggerFactory.getLogger(this.getClass)
  lazy implicit val formats = DefaultFormats + InstantSerializer

  override def serialize(value: SensorReading): Array[Byte] = {
    try {
      write(value).getBytes()
    } catch {
      case e: Exception =>
        LOG.warn("Error writing RawSensor", e)
        null
    }
  }
}

object RawSensorReadingDeserializationSchema extends AbstractDeserializationSchema[RawSensorReading] {

  private val LOG = LoggerFactory.getLogger(this.getClass)
  lazy implicit val formats = DefaultFormats + InstantSerializer

  override def deserialize(message: Array[Byte]): RawSensorReading = {
    try {
      val json = JsonMethods.parse(new ByteArrayInputStream(message))
      json.extract[RawSensorReading]
    } catch {
      case e: Exception =>
        LOG.warn("Error parsing RawSensorReading", e)
        null
    }
  }
}