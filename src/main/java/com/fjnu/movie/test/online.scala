package com.fjnu.movie.test

import com.fjnu.movie.bean.{MovieSame, RatingVO}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object online {
  def main(args: Array[String]): Unit = {
    //todo 累加器
    System.setProperty("hadoop.home.dir", "F:\\spark\\资料\\2.资料\\WindowsDep\\hadoop-3.0.0")
    // todo 准备环境
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("offline")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    //引入隐式转换
    import spark.implicits._
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))     // batch duration 批处理时间

    val MovieSameRDD= spark.read.format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", "movie_same")
      .load()
      .as[MovieSame]
      .rdd
      .cache()

    // 1.数据预处理 都换成map形式好转成矩阵
    val data= MovieSameRDD
      .map {
        case data => (data.mid, (data.same_mid, data.same_score))
      }
      .groupByKey()
      .map{
        case (a,b)=> (a,b.toMap)
      }
      .collectAsMap()

    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "movies",
      "auto.offset.reset" -> "latest"
    )
    // 通过kafka创建一个DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("moviesys"), kafkaParam)
    )

    // 对kafka传过来的数据进行处理
    val userData: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      msg => {
        val data = msg.value().split("\\|")
        // uid , mid , 用户评分score ,timestamp
        (data(0).toInt, data(1).toInt, data(2).toDouble, data(3).toInt)
      }
    }
    // 对已经处理好的数据进行计算 实时计算出相似的电影


  }
}
