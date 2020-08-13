package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_action {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    //action算子
    //reduce()
    //创建一个RDD,将所有元素聚合得到结果
    val rdd1:RDD[Int] = sparkContext.makeRDD(1 to 10,2)
    val rdd2 = sparkContext.makeRDD(Array(("a",1),("a",3),("b,5"),("d",6)))
    rdd1.reduce(_+_)
    //rdd2.reduce((x,y)=>(x._1 + y._1,x._2 + y._2))

    //collect()
    //创建一个RDD,并将RDD内容收集到Driver端打印
    val collectRDD = sparkContext.parallelize(1 to 10)
    collectRDD.collect()

    //count()
    //统计RDD的条数
    val countRDD = sparkContext.parallelize(1 to 10)
    countRDD.count()

    //first()
    val firstRDD = sparkContext.parallelize(1 to 10)
    firstRDD.first()

    //take(n)
    val takeRDD = sparkContext.parallelize(1 to 10)
    takeRDD.take(5)

    //takeOrdered(n)
    //返回该RDD排序后的前n个元素组成的数组
    val takeorderedRDD = sparkContext.parallelize(Array(2,5,4,6,9,1))
    takeorderedRDD.takeOrdered(3)

    //aggregate()
    //创建一个RDD，将所有元素相加得到结果
    val aggregateRDD = sparkContext.makeRDD(1 to 10,2)
    aggregateRDD.aggregate(0)(_+_,_+_)
    //Int = 55
    aggregateRDD.aggregate(10)(_+_,_+_)
    //Int = 85

    //save相关算子
    //将数据保存到不同格式的文件中
    //保存成Text文件
    rdd2.saveAsTextFile("output1")
    //保存成对象保存到文件
    rdd2.saveAsObjectFile("output2")
    //保存成Sequencefile文件
    //rdd2.map((_,1)).saveAsSequenceFile("output3")

    //foreach
    //分布式遍历RDD中的每一个元素，调用指定函数
    val rdd = sparkContext.makeRDD(List(1,2,3,4))
    rdd.map(num=>num).collect().foreach(println)

    sparkContext.stop()
  }
}
