package com.fjnu.movie.test

import com.fjnu.movie.bean.RatingVO
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

object offline {
  def main(args: Array[String]): Unit = {
    //todo 累加器
    System.setProperty("hadoop.home.dir", "F:\\spark\\资料\\2.资料\\WindowsDep\\hadoop-3.0.0")
    // todo 准备环境
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("offline")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    //引入隐式转换
    import spark.implicits._
    val ratingRDD= spark.read.format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", "rating")
      .load()
      .as[RatingVO]
      .rdd
      .cache()
    // 获得用户id
     val MidRDD = ratingRDD.map(_.mid).distinct().sample(false,0.01)
     val UidRDD = ratingRDD.map(_.uid).distinct().sample(false,0.1)

    // 构键Rating 模型；类
     val RatingModel = ratingRDD.map(data => Rating(data.uid,data.mid,data.score))
    //
    // 获得电影语义模型
    val model = ALS.train(RatingModel, 200, 5, 0.1)
/**    //用户和电电影id做笛卡尔积
    val testRDD= UidRDD.cartesian(MidRDD)
    val predictScore = model.predict(testRDD)

  //找出推荐的前20部电影
    val UserMovie: DataFrame = predictScore
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (data, items) => items.toList.sortWith(_._2 > _._2).take(20).map(items => (data, items._1, items._2))
      }
      .flatMap(data => data)
      .toDF("uid", "mid", "socre")

    insertTable(UserMovie,"u_m_top20")
    **/

    // todo 2.计算出电影的特征
    // 由于前面已经有电影的相似度矩阵
    val movieFeatures = model.productFeatures.map{ //获得电影特征
      case (mid,features) =>(mid,new DoubleMatrix(features))
    }.sample(true,0.01)
   // movieFeatures.collect().foreach(println)
    // 对所有电影计算他们的相似度，先做笛卡尔积
    val movieSame = movieFeatures.cartesian(movieFeatures)// (mid,[i,i...features])*(mid,[i,i...features])
      .filter{
        case (a,b) => a._1 != b._1  //去掉相同电影
      }
      .map{
        case (a,b) => {
           // 计算余弦相似度
          (a._1,(b._1,this.comsim(b._2,b._2)))   //形式:(mid , (mid , 相似度))
        }
      }
      .filter(_._2._2 > 0.5)
      .groupByKey()
      .map{
        case (mid ,items) => {items.toList.sortWith(_._2>_._2).map{
          items=>
          (mid,items._1,items._2)
        }}
      }
      .flatMap(data=>data)  //数据扁平化
      .toDF("mid","same_mid","same_score")
    insertTable(movieSame,"movie_same")


    //spark关闭
    spark.stop();
  }

  def  insertTable(df:DataFrame,tablename:String)={
    // 保存数据
    df.write
      .format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", tablename)
      .mode(SaveMode.Overwrite)  //  可选参数 ：Append,Overwrite,ErrorIfExists,Ignore;
      .save()
  }

  // 计算两个向量的余弦相似度
  def comsim(feature1:DoubleMatrix,feature2: DoubleMatrix):Double = {
    feature1.dot(feature2)/(feature1.norm2()*feature2.norm2())
  }
}
