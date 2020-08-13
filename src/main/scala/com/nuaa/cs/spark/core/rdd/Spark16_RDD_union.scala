package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_union {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    val dataRDD1:RDD[Int] = sparkContext.makeRDD(List(1,2,3,4))
    val dataRDD2:RDD[Int] = sparkContext.makeRDD(List(3,4,5,6))

    //intersection
    //求源RDD和参数RDD的交集后返回一个新的RDD
    val dataRDD3 = dataRDD1.intersection(dataRDD2)
    dataRDD3.collect().foreach(println)

    //union
    //对源RDD和参数RDD求并集后返回一个新的RDD
    val dataRDD4 = dataRDD1.union(dataRDD2)
    dataRDD4.collect().foreach(println)

    //subtract
    //以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来。求差集
    val dataRDD5 = dataRDD1.subtract(dataRDD2)
    dataRDD5.collect().foreach(println)

    //zip
    //将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的元素
    //两个RDD数据类型可以不一致
    //两个RDD数据分区要一致
    //两个RDD分区数量要一致
    val dataRDD6 = dataRDD1.zip(dataRDD2)
    dataRDD6.collect().foreach(println)
    sparkContext.stop()
  }
}
