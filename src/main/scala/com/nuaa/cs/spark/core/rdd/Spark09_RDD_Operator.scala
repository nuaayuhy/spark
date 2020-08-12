package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sparkContext.makeRDD(List(1,2,3,4),2)

    //转换（transformation）算子
    // 旧RDD => 算子 => 新 RDD，但是不会触发执行
    val rdd1:RDD[Int] = rdd.map( _ * 2 )

    rdd1.saveAsTextFile("output")
    //动作(action)算子
    //不会转换RDD，会触发作业的执行

    sparkContext.stop()
  }
}
