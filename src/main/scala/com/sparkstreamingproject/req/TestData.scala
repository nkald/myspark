package com.sparkstreamingproject.req

import java.time.{Instant, LocalDateTime}

object TestData {
  def main(args: Array[String]): Unit = {
    import java.time.format.DateTimeFormatter
    //获取日期
    val instant: Instant = Instant.ofEpochMilli(1594268203162L)
    println(instant)
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")



  }

}
