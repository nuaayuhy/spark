package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    // TODO Spark- 从磁盘中创建RDD
    // path:读取文件（目录）的路径
    //path可以设定相对路径，如果是IDEA,那么相对路径的位置从项目的根开始查找
    //path路径根据环境的不同自动发生改变

    //spark读取文件时，默认采用的是Hadoop读取文件（hdfs）
    //默认是一行一行的读取文件内容
    val fileRDD: RDD[String] = sc.textFile("input")
    //val fileRDD: RDD[String] = sc.textFile("hdfs://input/data.txt")
    fileRDD.collect().foreach(println)
    sc.stop()
  }
}
