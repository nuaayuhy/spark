package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_sortBy {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    //sortBy
    //使用func对数据处理。然后按照处理后的数据的比较结果进行排序
    val listRDD:RDD[Int] = sparkContext.makeRDD(List(1,2,2,3,4,1,8,8,6,6))

    val sortByRDD1: RDD[Int] = listRDD.sortBy(x=>x)

    val sortByRDD2: RDD[Int] = listRDD.sortBy(x=>x%3,false)

    sortByRDD1.collect().foreach(println)

    sortByRDD2.collect().foreach(println)

    sparkContext.stop()
  }
}
