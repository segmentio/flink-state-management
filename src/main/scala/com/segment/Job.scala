package com.segment

import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory

object Job {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val env = Pipeline.getEnvironment(params)
    env.execute("Sensor Reading Pipeline")
  }
}
