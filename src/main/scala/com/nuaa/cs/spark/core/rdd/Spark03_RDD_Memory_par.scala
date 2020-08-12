package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_par {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    //TODO Spark - 从内存中创建RDD，RDD中的分区数就是并行度，设定并行度=设定分区数量
    //1. makeRDD的第一个参数：数据源
    //2. makeRDD的第二个参数：默认并行度（分区的数量）
    //并行度默认会从spark配置信息中获取spark.default.parallelism值
    //如果获取不到指定参数，会采用默认值totalCores(机器的总核数)
    //local => 单核（单线程） => 1
    //local[4] => 4核 （4个线程） => 4
    //local[*] => 最大核数 => 8
    val dataRDD: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4), 4)
    val fileRDD: RDD[String] = sparkContext.textFile("input/word.txt", 2)
    fileRDD.collect().foreach(println)
    sparkContext.stop()
  }
}
