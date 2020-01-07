package com.segment

/**************************************************************
 * Simulate a way to determine if a sensor is enabled
 **************************************************************/
object SensorService {
  def sensorEnabled(id: String) = {
    id != "sensor1" && id != "sensor2"
  }
}