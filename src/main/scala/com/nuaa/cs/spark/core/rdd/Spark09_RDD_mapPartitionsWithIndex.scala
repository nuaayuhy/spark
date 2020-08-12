package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_mapPartitionsWithIndex {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // mapPartitionsWithIndex 关心分区号
    val listRDD:RDD[Int] = sparkContext.makeRDD(1 to 10,2)

    val indexRDD = listRDD.mapPartitionsWithIndex{
      case (num,data) => {
        data.map((_, "分区号:"+ num))
      }
    }

    indexRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
