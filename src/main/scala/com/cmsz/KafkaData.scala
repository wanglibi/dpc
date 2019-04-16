package com.cmsz


import java.sql.Connection

import com.alibaba.fastjson.JSON
import com.cmsz.util.{ConnectionPool, DateUtil, MathUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaData {

  val logger = Logger.getLogger(KafkaData.getClass)

  def main(args: Array[String]): Unit = {
    val args = Array("local[2]","test2","t","nd-00.hdp:6667","192.168.1.27:3306/dpc","root","123456")
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setMaster(args(0)).setAppName("DataProcessingCenter")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))


    val topics = Array(args(1))//"test2"

    val kafkaParams = Map(
      "bootstrap.servers" -> args(3),//"nd-00.hdp:6667"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> args(2),//t
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    try {
      val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
      logger.info("KafkaData.scala start completed in "+ (System.currentTimeMillis() - startTime)+" ms")

      val linesStream = kafkaStream.map(record => formatLogByJson(record)).reduceByKeyAndWindow((log1: (Int, Int, Int, Double, Int), log2: (Int, Int, Int, Double, Int)) => countLog(log1, log2), Seconds(60), Seconds(10))
      linesStream.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val conn: Connection = ConnectionPool.getConnection(args(4),args(5),args(6))
          partition.foreach(log => {
            val stmt = conn.createStatement
            stmt.execute(getSql(log))
          })
          ConnectionPool.returnConnection(conn)
        })
      })

      ssc.start()
      ssc.awaitTermination()

    } catch {
      case e: Exception => logger.error(e.getMessage)
    }


    //    kafkaStream.foreachRDD(rdd => {
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition(partition => {
    //        partition.foreach(record => {
    //          println(record)
    //        })
    //      })
    //      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    //    })

  }

  /**
    *
    * @param record 一条kafka json记录
    * @return （数量，数量，成功数,成功1失败0，金钱 ）
    */
  def formatLogByJson(record: ConsumerRecord[String, String]): (String, (Int, Int, Int, Double, Int)) = {
    try {
      //    if (System.currentTimeMillis() - record.timestamp() < 60 * 1000) {
      val originObject = JSON.parseObject(record.value)
      //returnCode
      val success = if (originObject.getString("returnCode") == "010A00") 1 else 0
      return (originObject.getString("paymentType"), (1, 1, success, success.toDouble, originObject.getString("payment").toInt))
      //    }
    }catch {
      case e:Exception => {logger.error("Record:("+record+")  "+e.getMessage);return ("",(0,0,0,0D,0))}
    }

  }

  //  var operatorData = (0, 0, 0, 1D, 0,"")

  /**
    *
    * @param log1 （数量，数量，成功数,成功率，金钱 ）
    * @param log2
    * @return 总数，tps，成功数，成功率，总金额,支付类型
    */
  def countLog(log1: (Int, Int, Int, Double, Int), log2: (Int, Int, Int, Double, Int)): (Int, Int, Int, Double, Int) = {
    try {
      val totalNum = log1._1 + log2._1
      val tps = totalNum / 60
      //无数据成功率为100%
      val successRate = if (totalNum != 0) MathUtil.divide(log1._3 + log2._3, totalNum) else 1

      return (totalNum, tps, (log1._3 + log2._3), successRate, log1._5 + log2._5)
    }catch{
      case e:Exception =>{logger.error("arg1:("+log1+")  arg2:("+log2+") "+e.getMessage);return (0,0,0,0,0)}
    }
  }

  def getSql(log: (String, (Int, Int, Int, Double, Int))): String = {
    val busTime = DateUtil.getCurrentMin
    val busType = 2 //1.订单查询 2支付 3退款 4退款查询

    //    if (recordCount < 0) {
    //      operatorData = (0, 0, 0, 1D, 0,"")
    //    }
    //operatorData(总数，tps，成功数，成功率，总金额)
    var sql = "insert into stream(business_type,payment_type,business_time,success_rate,tatoal_amount,trans_count,trans_tps) values ("
    sql += "'" + busType + "','" + log._1 + "','" + busTime + "'," + log._2._4 + "," + log._2._5 + "," + log._2._1 + "," + log._2._2 + ")"
    logger.info(sql)
    return sql
  }


  /**
    *
    * @param record 一条kafka记录
    * @return （数量，数量，成功数,成功1失败0，金钱 ）
    */
  //  def formatLog(record: ConsumerRecord[String, String]): (Int, Int, Int, Double, Int) = {
  //
  //    val logArray: Array[String] = record.value().split("\\|")
  //    val success = if (logArray(5) == "010A00") 1 else 0
  //    if (System.currentTimeMillis() - record.timestamp() < 60 * 1000) {
  //      return (1, 1, success, success, logArray(6).toInt)
  //    }
  //    return (0, 0, 0, 1D, 0)
  //  }
}
