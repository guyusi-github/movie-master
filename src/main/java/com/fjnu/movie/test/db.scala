package com.fjnu.movie

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import sun.util.calendar.BaseCalendar.Date

/**
 * 数据处理部分 : 1.把数据写入mysql 和es
 */
// 电影样例类
case class movie(mid:Int,movie_id:Int,rating:Double,ratings_count:Int,
                 title:String,original_title:String,aka_list:String,durations_list:String,
                 year:String,pubdates_list:String,movie_image:String,languages_list:String,
                 countries_list:String,writers_list_len:String,writer_id_list:String,
                 casts_list_len:String,cast_id_list:String,directors_list_len:String,
                 director_id_list:String,tags_list:String,genres_list:String,
                 writer_list:String,cast_list:String,director_list:String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )
object db{
  def main(args: Array[String]): Unit = {
    //todo 累加器
    System.setProperty("hadoop.home.dir", "F:\\spark\\资料\\2.资料\\WindowsDep\\hadoop-3.0.0")
    // todo 准备环境
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("Sql")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    //引入隐式转换
    import spark.implicits._
    //读取sql
    val df = spark.read.format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", "movie")
      .load()
      .as[movie]
      .toDF()
      .cache()
    df.createOrReplaceTempView("movie")
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册udf，把时间戳转换成年月格式
    //spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt )
    spark.udf.register("changeDate", ()=>System.currentTimeMillis().toInt )

    val ratingDF: DataFrame = spark.sql("select mid , rating as score  from movie ")
    //ratingDF.show()
    //模拟用户评分生产数据
    for (i <- 1001 to 2000){ //设置多少人看
      val time: Int = System.currentTimeMillis().toInt
      val ratingDf = ratingDF.rdd
        .filter( row => {row.getAs[Double]("score")!= 0})
        .sample(false,0.001) //随机采样
        .map{
          row => {
            var score = row.getAs[Double]("score")

            if(score !=0 && score > 3 && score <= 8 ){
              if (i %2 == 0) {
                score  = score + 1.toDouble
              }else{
                score  = score - 1.toDouble
              }
            }
            Rating(i, row.getAs[Int]("mid"), score, time)

          }}.toDF()
      commitRatingData(ratingDf)

    }


    //todo 把数据写入es
    //1 .创建es配置
    val settings = Settings.builder().put("cluster.name", "my-application").build()
    // 2. 创建客户端
    val client = new PreBuiltTransportClient(settings)
    // 3 .确定操作的机器配置
    client.addTransportAddress( new InetSocketTransportAddress(InetAddress.getByName("172.16.10.69"), 9300))
    // 4 . 创建访问es的index请求
    val IndexRequest: IndicesExistsRequest = new IndicesExistsRequest("moviesys_movie_index")
    // 5 . 客户端访问查看是否存在
    if (client.admin().indices().exists(IndexRequest).actionGet().isExists){
      //存在就删除
      client.admin().indices().delete(new DeleteIndexRequest("moviesys_movie_index"))
    }else{
      // 创建
      client.admin().indices().create(new CreateIndexRequest("moviesys_movie_index"))
    }
    //写入es
    df.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", "172.16.10.69:9200")
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode(SaveMode.Overwrite)
      .save("moviesys_movie_index/movie")
    // TODO 关闭环境
    spark.close()
  }

  // 把数据写入jdbc
  def commitRatingData(ratingDF:DataFrame) ={
    // 保存数据
    ratingDF.write
      .format("jdbc")
      .option("url","jdbc:mysql://172.16.10.254:3306/test4?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "secxun")
      .option("password", "secxun@2019")
      .option("dbtable", "rating")
      .mode(SaveMode.Append)  //  可选参数 ：Append,Overwrite,ErrorIfExists,Ignore;
      .save()                  //           追加     重写        存在就异常    忽略
  }
}
