package com.segment.message

//{
//  "id": "sensor1",
//  "timestamp": 1574370521000,
//  "value": 22.5
//}

case class SensorReadingKey(id: String, system: String)
case class RawSensorReading(key: SensorReadingKey, timestamp: Long, value: Double)
case class SensorReading(key: SensorReadingKey, timestamp: Long, value: Double, unit: String, name: String)