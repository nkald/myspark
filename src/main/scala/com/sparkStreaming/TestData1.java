package com.sparkStreaming;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestData1 {
    public static void main(String[] args) {
        //获取日期格式化对象
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd:HH-mm-ss");
        Instant instant = Instant.ofEpochMilli(1594268203162L);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String format = dateTimeFormatter.format(localDateTime);
        System.out.println(format);

        //将时间搓转换为日期



        //将日期装换为时间戳

//        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//
//        LocalDateTime parse = LocalDateTime.parse("2018-05-29 13:52:50", dateTimeFormatter);
//
//        long timeSp = LocalDateTime.from(parse)
//                .atZone(ZoneId.systemDefault())
//                .toInstant()
//                .toEpochMilli();
//
//        System.out.println(timeSp);
    }

}
