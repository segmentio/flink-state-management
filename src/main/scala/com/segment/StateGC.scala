package com.segment

import com.segment.message.Sensor
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.state.api.{OperatorTransformation, Savepoint}
import java.util.UUID.randomUUID
import org.slf4j.LoggerFactory

/**************************************************************
 * EnabledFilter is used to determine whether a sensor is enabled or not.
 **************************************************************/
object EnabledFilter extends FilterFunction[Tuple2[String, Sensor]] {
  override def filter(value: Tuple2[String, Sensor]): Boolean = SensorService.sensorEnabled(value.f0)
}

object StateGC {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    if (!isValidParams(params)) {
      return
    }

    val savepointDir = params.get("savepoint-dir", "")
    val savepointID = params.get("savepoint-id", "")
    val savepointPath = savepointDir + savepointID

    LOG.info(s"Loading from savepoint: ${savepointPath}")

    val env = ExecutionEnvironment.getExecutionEnvironment
    val savepoint = Savepoint.load(env.getJavaEnv, savepointPath, Pipeline.getBackend(params))

    /**************************************************************
     * Turn each operator state into datasets
     **************************************************************/
    val averagerKeyedState = savepoint.readKeyedState(ReadingAverager.uid, new ReadingAveragerStateFunction())
    val averagerBroadcastState = savepoint.readBroadcastState(
      ReadingAverager.uid,
      ReadingAverager.sensorMap.getName(),
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[Sensor] {})
    )
    val filterState = savepoint.readBroadcastState(
      ReadingFilter.uid,
      ReadingFilter.sensorDesc.getName(),
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[Sensor]() {})
    )

    /**************************************************************
     * Transform the states as Datasets to new instances of operators
     * with modified state.
     **************************************************************/
    val transformReadingFilter = OperatorTransformation
      .bootstrapWith(filterState.filter(EnabledFilter))
      .transform(new ReadingFilterBootstrapper)

    val transformAveragerState = OperatorTransformation
      .bootstrapWith(averagerKeyedState)
      .keyBy(new KeySelector[KeyedState, String] {
        override def getKey(state: KeyedState): String = state.key
      })
      .transform(new ReadingAveragerBootstrapFunction)

    /**************************************************************
     * Create the new savepoint
     **************************************************************/
    val newSavepoint = savepointDir + randomUUID
    
    Savepoint
      .create(Pipeline.getBackend(params), 128)
      .withOperator(ReadingFilter.uid, transformReadingFilter)
      .withOperator(ReadingAverager.uid, transformAveragerState)
      .write(newSavepoint)

    LOG.info(s"New savepoint created: ${newSavepoint}")

    env.execute("State GC")
  }

  def isValidParams(params: ParameterTool): Boolean = {
    var validParams = true

    if (params.get("checkpoint-dir", "") == "") {
      LOG.error("Missing required param: checkpoint-dir")
      validParams = false
    }

    if (params.get("savepoint-dir", "") == "") {
      LOG.error("Missing required param: savepoint-dir")
      validParams = false
    }

    if (params.get("savepoint-id", "") == "") {
      LOG.error("Missing required param: savepoint-id")
      validParams = false
    }

    if (!validParams) {
      LOG.error("""
      Usage:
        checkpoint-dir: directory to store checkpoints in
        savepoint-dir: directory to load and store savepoints
        savepoint-id: savepoint ID to load""")
    }

    validParams
  }
}