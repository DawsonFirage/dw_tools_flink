package com.dwsn.flinkApi.base;

import com.dwsn.flinkApi.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class DataStream05_Sink2File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("output")
                        , new SimpleStringEncoder<>("UTF-8"))
                // 文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 至少包含 15 分钟的数据
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 最近 5 分钟没有收到新的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                // 文件大小已达到 1 GB
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        // 将 Event 转换成 String 写入文件
        stream.map(Event::toString).addSink(fileSink);

        env.execute();

    }

}