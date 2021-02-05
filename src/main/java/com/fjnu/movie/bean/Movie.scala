package com.fjnu.movie.bean
// 电影字典用例类
case class Movie(mid:Int,movie_id:Int,rating:Double,ratings_count:Int,
                 title:String,original_title:String,aka_list:String,durations_list:String,
                 year:String,pubdates_list:String,movie_image:String,languages_list:String,
                 countries_list:String,writers_list_len:String,writer_id_list:String,
                 casts_list_len:String,cast_id_list:String,directors_list_len:String,
                 director_id_list:String,tags_list:String,genres_list:String,
                 writer_list:String,cast_list:String,director_list:String)
