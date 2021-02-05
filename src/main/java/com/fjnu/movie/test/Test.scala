package com.fjnu.movie.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\spark\\资料\\2.资料\\WindowsDep\\hadoop-3.0.0")
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("Sql")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    val sc: SparkContext = spark.sparkContext


      val names = List("张三", "李四", "王五")
      val scores = List(60, 70, 90)

      val namesRDD = sc.parallelize(names)
      val scoresRDD = sc.parallelize(scores)

      val cartesianRDD = namesRDD.cartesian(scoresRDD)

      cartesianRDD.foreach(tuple => {
        println("key:" + tuple._1 + "\tvalue:" + tuple._2)
      })
  }
}
