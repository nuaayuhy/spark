package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_textFile_PartitionData {


  def main(args: Array[String]): Unit = {

    // TODO: textFile分区数据存储
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // TODO：文件分区    文件分几个区？  文件分区规则：以字节方式来分区
    // textFile 第一个参数表示读取文件的路径
    // textFile 第二个参数表示最小分区数量    默认值为：math.min(defaultParallelism,2)
    // 所谓的最小分区数，取决于总的字节数是否能够整除分区数并且剩余的字节达到一个比率
    // 实际产生的分区数量可能大于最小分区数

    // TODO：数据读取 分区的数据如何存储？  数据读取规则：以行为单位来读取，但会考虑数据的偏移量offset
    val fileRDD: RDD[String] = sparkContext.textFile("input/word.txt", 2)
    fileRDD.saveAsTextFile("output")
    sparkContext.stop()
  }
}
