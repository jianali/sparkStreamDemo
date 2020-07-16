package com.lj
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import kafka.serializer.StringDecoder
/**
 * SparkStream对接TDH测试
 * added by jianli
 */
object App {
  def main(args: Array[String]): Unit = {
    println("Hello World!")
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingKafka_Direct")
      .setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //设置每个批次间隔时间为1s
    val ssc = new StreamingContext(sc, Seconds(1))
    //设置checkpoint目录
    ssc.checkpoint("./Kafka_Direct")
    // 4.1.配置kafka相关参数
    val kafkaParams=Map("metadata.broker.list"->"172.26.4.29:9092,172.26.4.30:9092,172.26.4.33:9092","group.id"->"kafka_Direct")
    // 4.2.定义topic
    val topics=Set("kafka_spark")
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    // 5.获取topic中的数据
    val topicData: DStream[String] = dstream.map(_._2)
    // 6.切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_,1))
    // 7.相同单词出现的次数累加
    val resultDS: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    // 8.通过Output Operations操作打印数据
    resultDS.print()
    // 9.开启流式计算
    ssc.start()
    // 阻塞一直运行
    ssc.awaitTermination()
  }
}
