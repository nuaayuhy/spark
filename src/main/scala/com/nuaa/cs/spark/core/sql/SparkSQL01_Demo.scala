package com.nuaa.cs.spark.core.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    //SparkSQl

    //SparkConf
    val  sparkconf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    val spark:SparkSession =SparkSession.builder().config(sparkconf).getOrCreate()


    //读取json文件，创建DataFrame
    val frame:DataFrame = spark.read.json("input/user.json")
    frame.show()

    //SQL
    frame.createGlobalTempView("user")
    spark.sql("select avg(age) from user").show()

    //DSL
    frame.select("username","age").show()

    //*****RDD=>DataFrame=>DataSet*****
    //如果需要RDD与DF或者DS之间操作，需要引入隐式转换规则
    //这里的spark不是包名的含义，而是SparkSession对象名
    import spark.implicits._

    //RDD
    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",20)))

    //DataFrame
    val df1: DataFrame = rdd1.toDF("id","name","age")
    //df1.show()

    //DateSet
    val ds1: Dataset[User] = df1.as[User]
    //ds1.show()

    //*****DataSet=>DataFrame=>RDD*****
    //DataFrame
    val df2: DataFrame = ds1.toDF()

    //RDD  返回的RDD类型为Row，里面提供的getXXX方法可以获取字段值，类似jdbc处理结果集，但是索引从0开始
    val rdd2: RDD[Row] = df2.rdd
    //rdd2.foreach(a=>println(a.getString(1)))

    //*****RDD=>DataSet*****
    rdd1.map{
      case (id,name,age)=>User(id,name,age)
    }.toDS()

    //*****DataSet=>=>RDD*****
    ds1.rdd

    spark.stop()


  }
}
case class User(id:Int,name:String,age:Int)
