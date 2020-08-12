package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_groupBy {


  def main(args: Array[String]): Unit = {

    // TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    val listRDD:RDD[List[Int]] = sparkContext.makeRDD(List(1,2,3,4))

    // groupBy
    // 作用：分组，按照传入函数的返回值进行分组，将相同的key对应的值放入一个迭代器
    //生成数据，按照指定的规则进行分组
    //分组后的数据形成了对偶元组（K-V）
    val groupByRDD:RDD[(Int, Iterable[Int])] = listRDD.groupBy(i=>i%2)

    groupByRDD.collect().foreach(println)

    sparkContext.stop()
  }
}
