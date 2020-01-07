package com.segment

import com.segment.message.{RawSensorReading, RawSensorReadingDeserializationSchema, Sensor, SensorDeserializationSchema, SensorReading, SensorReadingSerializationSchema}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.connectors.rabbitmq.{RMQSink, RMQSource}
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.apache.flink.streaming.api.scala._

object Pipeline {
  def getBackend(params: ParameterTool): StateBackend = {
    val checkpointLocation = params.get("checkpoint-dir", "")
    new RocksDBStateBackend(checkpointLocation, true)
  }

  def getEnvironment(params: ParameterTool): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    /*****************************
     *  Checkpoint config
     *****************************/
    env.setStateBackend(this.getBackend(params))
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // make sure 500 ms of progress happen between checkpoints
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)

    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    /*****************************
     *  RabbitMQ Config
     *****************************/
    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost(params.get("rabbitmq", "127.0.0.1"))
      .setPort(5672)
      .setVirtualHost("/")
      .setUserName("guest")
      .setPassword("guest")
      .build

    val sensorReadingStream = new RMQSource[RawSensorReading](
      connectionConfig,
      "sensor-readings",
      false,
      RawSensorReadingDeserializationSchema)

    val sensorStream = new RMQSource[Sensor](
      connectionConfig,
      "sensors",
      false,
      SensorDeserializationSchema)

    /*****************************
     *  RabbitMQ Input
     *****************************/
    val readingData: DataStream[RawSensorReading] = env
      .addSource(sensorReadingStream)
      .assignTimestampsAndWatermarks(new IngestionTimeExtractor[RawSensorReading])
      .setParallelism(1)
      .name("SensorReadingIngest")
      .uid("sensor-reading-ingest")

    val sensorDetails: DataStream[Sensor] = env
      .addSource(sensorStream)
      .assignTimestampsAndWatermarks(new IngestionTimeExtractor[Sensor])
      .setParallelism(1)
      .name("SensorIngest")
      .uid("sensor-ingest")

    /*****************************
     *  Message Processing
     *****************************/
    val parsedSensorDetails = sensorDetails.flatMap(SensorManager)

    val readingFilter = readingData
      .connect(parsedSensorDetails.broadcast(ReadingFilter.broadcastStates: _*))
      .process(new ReadingFilter)
      .name(ReadingFilter.name)
      .uid(ReadingFilter.uid)

    val readingAvg = readingFilter
      .keyBy(_.key)
      .connect(parsedSensorDetails.broadcast(ReadingAverager.broadcastStates: _*))
      .process(new ReadingAverager)
      .name(ReadingAverager.name)
      .uid(ReadingAverager.uid)

    /*****************************
     *  Output
     *****************************/
    readingAvg.addSink(new RMQSink[SensorReading](
      connectionConfig,
      "output",
      new SensorReadingSerializationSchema))
      .name("SensorOutput")
      .uid("sensor-output")

    env
  }
}
