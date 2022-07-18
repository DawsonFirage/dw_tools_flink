package com.dwsn.sql.base;

import com.dwsn.sql.pojo.Event;
import com.dwsn.sql.utils.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SQL01_SimpleExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1 读取数据，得到DataStream
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>)
                                (element, recordTimestamp) -> element.timestamp));

        // 2 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3 将 DataStream 转换成 Table
        Table eventTable = tableEnv.fromDataStream(stream);

        // 4 SQL 语法转换
        Table resultTable1 = tableEnv.sqlQuery("select user, url, `timestamp` from " + eventTable);

        // 5 基于Table
        Table resultTable2 = eventTable.select($("user"), $("url"), $("timestamp"))
                .where($("user").isEqual("Alice"));

        // 6 转换成流打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        env.execute();
    }
}
