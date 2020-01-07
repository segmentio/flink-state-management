package com.segment.message

//{
//  "id": "sensor1",
//  "timestamp": 1574370521000,
//  "value": 22.5
//}

case class RawSensorReading(id: String, timestamp: Long, value: Double)
case class SensorReading(id: String, timestamp: Long, value: Double, unit: String, name: String)