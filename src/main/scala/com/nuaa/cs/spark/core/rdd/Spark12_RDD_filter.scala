package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_filter {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // filter
    // 生成数据，按照指定的规则进行过滤
    val listRDD= sparkContext.makeRDD(List(1,2,3,4))

    val filterRDD = listRDD.filter(x=>x%2==0)

    filterRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
