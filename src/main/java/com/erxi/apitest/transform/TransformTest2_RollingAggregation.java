package com.erxi.apitest.transform;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author AKA二夕
 * @create 2021-02-09 15:26
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中获取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型，
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 按照 sensor_id 字段名称，进行选取分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());

        // 直接获取数据元素
        DataStream<Long> dataStream1 = env.fromElements(1L, 34L, 4L, 657L, 23L);

        /*
         * Long 表示需要获取元素的类型
         * Integer 表示经过函数转换之后的元素类型
         */
        KeyedStream<Long, Integer> keyedStream2 = dataStream1.keyBy(new KeySelector<Long, Integer>() {
            /**
             * @param value 获取需要处理的元素
             * @return 经过处理之后的 value
             * @throws Exception 处理异常
             */
            @Override
            public Integer getKey(Long value) throws Exception {
                return value.intValue() % 2;
            }
        });

        // 滚动聚合，取当前温度的最大值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        // 打印
        resultStream.print("result");
        keyedStream.print("key");
        keyedStream2.sum(0).print("key2");

        // 执行
        env.execute();

    }
}
