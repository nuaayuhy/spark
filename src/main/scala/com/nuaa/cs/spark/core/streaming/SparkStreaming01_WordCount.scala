package com.nuaa.cs.spark.core.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {

    // 使用netcat工具向9999端口不断的发送数据
    // 通过SparkStreaming读取端口数据并统计不同单词出现的次数

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams = ssc.socketTextStream("localhost", 9999)

    //将每一行数据做切分，形成一个个单词
    val wordStreams = lineStreams.flatMap(_.split(" "))

    //将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    //打印
    wordAndCountStreams.print()

    //启动SparkStreamingContext采集器
    ssc.start()
    //等待采集器的结束
    ssc.awaitTermination()

  }

}
