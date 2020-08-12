package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_flatMap {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    val listRDD:RDD[List[Int]] = sparkContext.makeRDD(Array(List(1,2), List(3,4)))

    // flatMap
    // 1,2,3,4
    val flatMapRDD:RDD[Int] = listRDD.flatMap(data => data)

    flatMapRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
