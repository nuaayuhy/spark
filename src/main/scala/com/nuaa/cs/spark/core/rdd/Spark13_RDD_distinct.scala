package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_distinct {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // distinct 去重
    //因为去重后数据减少，所以可以调整默认数据分区数
    //将rdd中一个分区的数据打乱重组到其他不同的分区的操作，称为shuffle
    //spark中所有转换算子中没有shuffle的操作，性能较快
    val listRDD:RDD[Int] = sparkContext.makeRDD(List(1,2,2,3,4,1,8,8,6,6))

    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    distinctRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
