package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator2 {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sparkContext.makeRDD(List(1,2,3,4),2)

    val rdd1:RDD[Int] = rdd.map(x =>{
      println("x map A = "+ x)
      x
    })

    val rdd2:RDD[Int] = rdd1.map(x =>{
      println("x map B = "+ x)
      x
    })

    println(rdd2.collect().mkString(","))

    sparkContext.stop()
  }
}
