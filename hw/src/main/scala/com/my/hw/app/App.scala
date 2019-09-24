package com.my.hw.app

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Properties
import java.util.concurrent.CountDownLatch

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{Grouped, Materialized}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}


case class JsonMapper(mapper: ObjectMapper) extends Serializable

object JsonMapper {
  def apply(): JsonMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
    mapper.configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    JsonMapper(mapper)
  }
}


case class TaskUnitG(tUnitId: String, ts: Long, isConf: Int)

case class Task(taskId: String, lastUpdateTs: Long, confNumCnt: Int)

object Task {
  def empty: Task = Task("", 0L, 0)
}

object TaskSerdes extends Serializable {

  def serialize(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[T]
  }

  val taskUnitGSerde: Serde[TaskUnitG] = Serdes.fromFn(
    obj => serialize(obj),
    bytes => Some(deserialize[TaskUnitG](bytes)))

  val taskSerde: Serde[Task] = Serdes.fromFn(
    obj => serialize(obj),
    bytes => Some(deserialize[Task](bytes)))
}

object App {
  def main(args: Array[String]): Unit = {

    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "homework_v0.81")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
      p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 500)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p
    }

    implicit val groupedTaskUnit: Grouped[String, TaskUnitG] = Grouped.`with`(Serdes.String, TaskSerdes.taskUnitGSerde)
    implicit val groupedTask: Grouped[String, Task] = Grouped.`with`(Serdes.String, TaskSerdes.taskSerde)

    implicit val materializedTaskUnit: Materialized[String, TaskUnitG, ByteArrayKeyValueStore] = {
      Materialized.`with`[String, TaskUnitG, KeyValueStore[Bytes, Array[Byte]]](
        Serdes.String,
        TaskSerdes.taskUnitGSerde
      )
    }
    implicit val materializedTask: Materialized[String, Task, ByteArrayKeyValueStore] = {
      Materialized.`with`[String, Task, KeyValueStore[Bytes, Array[Byte]]](
        Serdes.String,
        TaskSerdes.taskSerde
      )
    }

    val mapper = JsonMapper().mapper
    val builder: StreamsBuilder = new StreamsBuilder
    // set up inputStream
    val inputStream = builder.stream[String, String]("homework").mapValues(v =>
      // parse input Json from topic
      mapper.readValue(v, classOf[Map[String, String]])
    )

    //create output stream
    val outputStream = inputStream
      // filter only valid rows
      .filter((_, v) => v("op") == "c")
      .mapValues(v => {
        // parse inner json from "after" object
        val parsedNode = mapper.readValue(v("after"), classOf[Map[String, Any]])
        (v("ts_ms").asInstanceOf[Long], parsedNode)
      })
      .mapValues(v => {
        // check if segment is confirmed
        val lvl = v._2("levels").asInstanceOf[List[Int]].head
        val confLvl = v._2("tUnits").asInstanceOf[List[Map[String, String]]].head("confirmedLevel").asInstanceOf[Int]
        val tUnit = v._2("tUnits").asInstanceOf[List[Map[String, String]]].head("tUnitId")
        val isConf = if (lvl == confLvl) 1 else 0
        TaskUnitG(tUnit, v._1, isConf)
      })
      // take last state of segment by operation timestamp
      .groupBy((_, v) => v.tUnitId)(groupedTaskUnit)
      .reduce((v1, v2) => if (v1.ts > v2.ts) v1 else v2)(materializedTaskUnit)
      .mapValues(v => {
        // get Task ID for group
        val lTaskId = v.tUnitId.split(":").head
        Task(lTaskId, v.ts, v.isConf)
      })
      // aggregate => count number of confirmed segments for TaskId
      .groupBy((_, v) => (v.taskId, v))(groupedTask)
      .aggregate(Task.empty)(
        (_, newTks, aggTsk) => {
        // check if this is a new task by timestamp
        if (newTks.lastUpdateTs > aggTsk.lastUpdateTs)
          Task(newTks.taskId, newTks.lastUpdateTs, aggTsk.confNumCnt + newTks.confNumCnt)
        else aggTsk
      }
        , (_, _, aggTsk) => aggTsk
      )(materializedTask)
      .toStream
      .map((_, v) => (v.taskId, v.confNumCnt))

    // check the result:
    outputStream.print(Printed.toSysOut[String, Int].withLabel("homework_result"))

    // save the result to topic
    outputStream.to("homework_result")

    val topology = builder.build()
    println(topology.describe())

    val streams: KafkaStreams = new KafkaStreams(topology, props)
    val latch = new CountDownLatch(1)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })

    try {
      streams.cleanUp()
      streams.start()
      latch.await()
    } catch {
      case e: Throwable =>
        println(e)
        System.exit(1)
    }
    System.exit(0)
  }
}