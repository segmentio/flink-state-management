package com.segment.message

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.segment.json.InstantSerializer
import org.json4s.DefaultFormats
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SerializationSchema}
import org.json4s.native.JsonMethods
import org.slf4j.LoggerFactory
import org.json4s.native.Serialization.write

class SensorSerializationSchema extends SerializationSchema[Sensor] {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  lazy implicit val formats = DefaultFormats + InstantSerializer

  override def serialize(value: Sensor): Array[Byte] = {
    try {
      write(value).getBytes()
    } catch {
      case e: Exception =>
        LOG.warn("Error writing Sensor", e)
        null
    }
  }
}

object SensorDeserializationSchema extends AbstractDeserializationSchema[Sensor] {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  lazy implicit val formats = DefaultFormats + InstantSerializer

  override def deserialize(message: Array[Byte]): Sensor = {
    try {
      val json = JsonMethods.parse(new ByteArrayInputStream(message))
      json.extract[Sensor]
    } catch {
      case e: Exception =>
        val asString = new String(message, StandardCharsets.UTF_8)
        LOG.warn(s"Error parsing Sensor ($asString)", e)
        null
    }
  }
}

class SensorKryoSerializer extends Serializer[Sensor] {
  override def write(kryo: Kryo, output: Output, `object`: Sensor): Unit = {
    val s = new SensorSerializationSchema()
    output.write(s.serialize(`object`))
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Sensor]): Sensor = {
    SensorDeserializationSchema.deserialize(input.getBuffer)
  }
}