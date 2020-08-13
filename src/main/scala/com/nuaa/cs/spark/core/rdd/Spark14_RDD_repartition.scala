package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_repartition {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    //coalesce重新分区，可以选择是否进行shuffle过程。由参数shuffle:Boolean-false/true决定
    // repartition实际上调用的coalesce,默认是进行shuffle的
    val listRDD:RDD[Int] = sparkContext.makeRDD(List(1,2,2,3,4,1,8,8,6,6))

    //listRDD.coalesce()
    val repartitionRDD: RDD[Int] = listRDD.repartition(2)

    repartitionRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
