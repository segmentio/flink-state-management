//package com.segment.message
//
//import java.io.ByteArrayInputStream
//
//import com.esotericsoftware.kryo.{Kryo, Serializer}
//import com.esotericsoftware.kryo.io.{Input, Output}
//import com.segment.json.InstantSerializer
//import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SerializationSchema}
//import org.json4s.DefaultFormats
//import org.json4s.native.JsonMethods
//import org.json4s.native.Serialization.write
//import org.slf4j.LoggerFactory
//
//class SensorKeySerializationSchema extends SerializationSchema[SensorKey] {
//  private val LOG = LoggerFactory.getLogger(this.getClass)
//  lazy implicit val formats = DefaultFormats + InstantSerializer
//
//  override def serialize(value: SensorKey): Array[Byte] = {
//    try {
//      write(value).getBytes()
//    } catch {
//      case e: Exception =>
//        LOG.warn("Error writing SensorKey", e)
//        null
//    }
//  }
//}
//
//object SensorKeyDeserializationSchema extends AbstractDeserializationSchema[SensorKey] {
//  private val LOG = LoggerFactory.getLogger(this.getClass)
//  lazy implicit val formats = DefaultFormats + InstantSerializer
//
//  override def deserialize(message: Array[Byte]): SensorKey = {
//    try {
//      val json = JsonMethods.parse(new ByteArrayInputStream(message))
//      json.extract[SensorKey]
//    } catch {
//      case e: Exception =>
//        LOG.warn(s"Error parsing SensorKey (${message.toString()})", e)
//        null
//    }
//  }
//}
//
//class SensorKeyKryoSerializer extends Serializer[SensorKey] {
//  override def write(kryo: Kryo, output: Output, `object`: SensorKey): Unit = {
//    val s = new SensorKeySerializationSchema()
//    output.write(s.serialize(`object`))
//  }
//
//  override def read(kryo: Kryo, input: Input, `type`: Class[SensorKey]): SensorKey = {
//    SensorKeyDeserializationSchema.deserialize(input.getBuffer)
//  }
//}