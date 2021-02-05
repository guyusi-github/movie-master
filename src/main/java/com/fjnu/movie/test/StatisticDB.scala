package com.fjnu.movie.test

import java.text.SimpleDateFormat
import java.util.Date

import com.fjnu.movie.bean.{ Movie, Rating}
import com.fjnu.movie.test.StatisticDB.insertTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StatisticDB {
  def main(args: Array[String]): Unit = {
    //todo 累加器
    System.setProperty("hadoop.home.dir", "F:\\spark\\资料\\2.资料\\WindowsDep\\hadoop-3.0.0")
    // todo 准备环境
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("Sql")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    //引入隐式转换
    import spark.implicits._
    //读取sql
    val movieDF= spark.read.format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", "movie")
      .load()
      .as[Movie]
      .toDF()
      .cache()
    movieDF.createOrReplaceTempView("movie")
    val ratingDF= spark.read.format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", "rating")
      .load()
      .as[Rating]
      .toDF()
      .cache()
    ratingDF.createOrReplaceTempView("ratings")

    // todo 1.统计被看的电影多少
   // val midCountDF: DataFrame = spark.sql("select mid, count(mid) as mid_count from ratings group by mid ")
    // 写入u_m_conut 中
   // midCountDF.show()
    //insertTable(midCountDF,"u_m_count")
    // todo 2.统计热门电影 每个月的排名 又俺评分排名
    // 创建一个日期格式化工具
   // val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf，把时间戳转换成年月格式
   // spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt )

   // val HotMovieDF =spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
   // HotMovieDF.createOrReplaceTempView("ratingyearmonth")
    //val hotMoviesDF: DataFrame = spark.sql("select mid, count(mid) as mid_count, yearmonth from ratingyearmonth group by yearmonth, mid order by yearmonth desc, mid_count desc")
    //HotMovieDF.show()
   // insertTable(hotMoviesDF,"hot_movie")
    // todo 3. 求评分电影的平均分
    // 3. 优质电影统计，统计电影的平均评分，mid，avg
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg_score from ratings group by mid")
    //insertTable(averageMoviesDF, "avg_movie")
    // todo 4.统计各个类别的热门电影
    val movieWithScore = movieDF.join(averageMoviesDF, "mid") //根据两个电影id，加上另一个表的avg_score
    //movieWithScore.show()
    // 1.电影类型
    val  genresRDD =  spark.sparkContext.makeRDD(List("剧情","喜剧","动作","爱情","科幻","动画","悬疑","惊悚","恐怖","犯罪","同性","音乐","歌舞","传记","历史","战争","西部","奇幻","冒险","灾难","武侠","情色"))
    // 2.和电影数据做笛卡尔积
    val genresTopMoviesDF = movieWithScore.rdd.cartesian(genresRDD)
      .filter{ //做模式匹配要有标签的
    case  (movieRow,genre)=> movieRow.getAs[String]("genres_list").contains(genre)
    }.map{
      case (movieRow,genre) =>  (genre,(movieRow.getAs[Int]("mid"),movieRow.getAs[Double]("avg_score")))
      //case (movieRow,genre) =>  (genre)
    }
      .groupByKey()
      .map{
        case (genre,items) => {
           items.toList.sortWith(_._2 > _._2).take(10).map(data => {
          (genre, data._1, data._2)
          })
         // insertTable(frame,"genres_top10")
        }

      }.flatMap(data=>data).toDF("genres","mid","score")
   // genresTopMoviesDF.show()
    insertTable(genresTopMoviesDF,"genres_top10")




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
}
