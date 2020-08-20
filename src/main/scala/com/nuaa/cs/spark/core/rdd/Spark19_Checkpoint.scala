package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Checkpoint {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setCheckpointDir("cp")
    val rdd = sparkContext.makeRDD(List(1,2,3,4))

    val mapRDD = rdd.map((_,1))

    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_+_)

    reduceRDD.foreach(println)

    println(reduceRDD.toDebugString)

    sparkContext.stop()
  }
}
