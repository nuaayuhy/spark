package com.nuaa.cs.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

object Spark17_RDD_keyvalue {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // partitionBy
    // 将数据按照指定Partitioner重新分区, spark默认使用HashPartitioner
    // 可以自定义分区器
    val rdd1: RDD[(Int, String)] = sparkContext.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)
    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new HashPartitioner(2))
    rdd2.saveAsTextFile("output")

    // reduceByKey
    // 可以将数据按照相同的Key对Value进行聚合
    val dataRDD1 = sparkContext.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = dataRDD1.reduceByKey(_+_)
    val dataRDD3 = dataRDD1.reduceByKey(_+_, 2)

    // groupByKey
    // 将分区的数据直接转换为相同类型的内存数组进行后续处理
    val rdd3 = sparkContext.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd4 = rdd3.groupByKey()
    val rdd5 = rdd3.groupByKey(2)
    val rdd6 = rdd3.groupByKey(new HashPartitioner(2))

    // TODO reduceBy和groupBy的区别
    // groupBy:按照Key进行分组，直接进行分组
    // reduceByKey在shuffle之前有预聚合（combine）操作，返回结果是RDD[k,v]

    // aggregateByKey
    // 将数据根据不同的规则进行分区内计算和分区间计算
    // TODO : 取出每个分区内相同key的最大值然后分区间相加
    // aggregateByKey算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
    //    2.1 第一个参数表示分区内的计算规则
    //    2.2 第二个参数表示分区间的计算规则
    val rdd = sparkContext.makeRDD(List(
      ("a",1),("a",2),("c",3),
      ("b",4),("c",5),("c",6)),2)
    // 0:("a",10),("a",2),("c",3) => (a,10)(c,10)
    //                                         => (a,10)(b,10)(c,20)
    // 1:("b",4),("c",5),("c",6) => (b,10)(c,10)
    // 初始的分区内的key值设置为10
    val resultRDD = rdd.aggregateByKey(10)(
        (x, y) => math.max(x,y),
        (x, y) => x + y)
        //(x, y) => math.max(_,_),
        //(x, y) => _ + _)
    resultRDD.collect().foreach(println)

    // foldByKey
    // 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
    val foldRDD1 = sparkContext.makeRDD(List(("a",1),("b",2),("c",3)))
    val foldRDD2 = foldRDD1.foldByKey(0)(_+_)


    // combineByKey
    // 对相同K,把V合并成一个集合
    // 案例：求平均值
    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)

    val combineRdd: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    val result = combineRdd.map{ case (key,value) => (key,value._1/value._2.toDouble)}
    result.collect().foreach(println)

    //sortByKey
    //在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的（K，V）的RDD

    val sortRDD1 = sparkContext.makeRDD(List(("a",1),("b",2),("c",3)))
    val sortRDD2: RDD[(String, Int)] = sortRDD1.sortByKey(true)
    val sortRDD3: RDD[(String, Int)] = sortRDD1.sortByKey(false)

    sparkContext.stop()
  }
}
