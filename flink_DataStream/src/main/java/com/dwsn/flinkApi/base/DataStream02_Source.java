package com.dwsn.flinkApi.base;

import com.dwsn.flinkApi.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.ArrayList;

public class DataStream02_Source {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 在集合中读取
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));

        DataStream<Event> stream = env.fromCollection(clicks);
        stream.print("collection");

        // 2 直接读取元素
        DataStreamSource<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        stream2.map(event -> event.user).print("element");

        // 3 从文件读取数据
        DataStream<String> stream3 = env.readTextFile("input/words.txt");
        stream3.print("file");

        // 4 从 Socket 读取数据
        DataStream<String> stream4 = env.socketTextStream("localhost", 7777);
        stream4.print("socket");

        env.execute();

    }

}