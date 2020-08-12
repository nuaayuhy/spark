package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Memory_PartitionData {


  def main(args: Array[String]): Unit = {

    // TODO: RDD分区数据存储
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 内存中的集合数据按照平均分的方式进行分区处理
    val rdd: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4), 2)
    rdd.saveAsTextFile("output")

    val rdd1: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4), 3)
    rdd1.saveAsTextFile("output1")

    // TODO 内存中的集合数据如果不能平均分，会采用一个算法实现
    //makeRDD() -> ParallelCollectionRDD() -> slice() -> match positions -> array.slice()

    val rdd2: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4), 4)
    rdd2.saveAsTextFile("output2")
    sparkContext.stop()
  }
}
