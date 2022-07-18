package com.dwsn.flinkApi.base;

import com.dwsn.flinkApi.pojo.Event;
import com.dwsn.flinkApi.utils.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class DataStream09_Window {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置生成水位线的时间间隔。默认来一条数据生成一次水位线。
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的 Watermark 生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.map(event -> Tuple2.of(event.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 滑动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 事件时间会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                // 滑动记数窗口
//                .countWindow(10, 2)
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .print();


        env.execute();
    }

}