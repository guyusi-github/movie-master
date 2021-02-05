package com.fjnu.movie.enume

object JDBCConfig extends Enumeration {
     val URL = Value("jdbc:mysql://172.16.10.254:3306/test?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull")

}
object tet{
  def main(args: Array[String]): Unit = {
    println(JDBCConfig.URL)
    println(JDBCConfig)

  }
}