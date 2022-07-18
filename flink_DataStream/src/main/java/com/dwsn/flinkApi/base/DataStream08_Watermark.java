package com.dwsn.flinkApi.base;

import com.dwsn.flinkApi.pojo.Event;
import com.dwsn.flinkApi.utils.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class DataStream08_Watermark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置生成水位线的时间间隔。默认来一条数据生成一次水位线。
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L))
                // 设置水位线：离源越近越好
                // Type1 有序流的 watermark 生成
                // 通常有序流只用于测试，实际生产中都是乱序流
//                .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        // 指定如何从数据中提取 事件时间
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                // 返回数据中的时间戳字段，单位为毫秒
//                                return element.timestamp * 1000;
//                            }
//                        }) )
                // Type2 乱序流的 Watermark 生成
                // 水位线策略选择 forBoundedOutOfOrderness ，指定数据延迟时间（如例子为两秒钟）
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // 指定如何从数据中提取 事件时间
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                // 返回数据中的时间戳字段，单位为毫秒
                                return element.timestamp * 1000;
                            }
                        })
                );


        env.execute();

    }

}