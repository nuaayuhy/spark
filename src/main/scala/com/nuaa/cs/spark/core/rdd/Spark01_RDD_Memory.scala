package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO Spark- 从内存中创建RDD
    // 1.parallelize 并行
    val list = List(1,2,3,4)
    val rdd:RDD[Int] = sc.parallelize(list)
    println(rdd.collect().mkString(","))

    //2.makeRDD
    val rdd1:RDD[Int] = sc.makeRDD(list)
    println(rdd1.collect().mkString(","))

    sc.stop()
  }
}
