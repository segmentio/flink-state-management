package com.segment

import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory

object Job {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    if (!isValidParams(params)) {
      return
    }

    val env = Pipeline.getEnvironment(params)
    env.execute("Sensor Reading Pipeline")
  }

  def isValidParams(params: ParameterTool): Boolean = {
    var validParams = true

    if (params.get("checkpoint-dir", "") == "") {
      LOG.error("Missing required param: checkpoint-dir")
      validParams = false
    }

    if (!validParams) {
      LOG.error("""
      Usage:
        checkpoint-dir: directory to store checkpoints in""")
    }

    validParams
  }
}
