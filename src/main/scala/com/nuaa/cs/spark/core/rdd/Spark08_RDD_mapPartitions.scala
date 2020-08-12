package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_mapPartitions {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // mapPartitions
    val listRDD:RDD[Int] = sparkContext.makeRDD(List(1,2,3,4),2)

    // mapPartitions可以对一个RDD中所有的分区进行遍历
    // mapPartitions效率优于map算子，减少了发送到执行器执行交互次数（IO开销）
    // mapPartitions可能会出现内存溢出(Out Of Memory)
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(data=>{
      data.map(data=>data * 2)
    })

    mapPartitionsRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
