package com.erxi.apitest.transform;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author AKA二夕
 * @create 2021-02-10 18:43
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1、分流，按照温度值30度为界分为两流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            /**
             * @param value 应选择输出对象的输出对象。
             * @return 如果温度大于 30 度，返回 high，否则返回 low
             */
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("all");

        highTempStream.print("high");
        lowTempStream.print("low");
//        allTempStream.print("all");

        // 2、合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            /**
             * @param value 输入的值
             * @return 二元组类型：sensor_id, temperature
             * @throws Exception 抛出异常
             */
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        // 将高温流和低温流进行合流，得到 connectStream
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream.connect(lowTempStream);

        // 对合流之后的数据进行操作，分别处理低温流和高温流
        DataStream<Object> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            /**
             * 传感器温度过高的情况
             * @param value 经过合流操作之后的二元组类型，用于获取 sensor_id 和 温度
             * @return 直接返回 sensor_id，温度和报警信息
             * @throws Exception 抛出异常
             */
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            /**
             * 传感器温度正常的情况
             * @param value {@link SensorReading} 的类型元素
             * @return 直接返回 sensor_id 和 normal 即可
             * @throws Exception 抛出异常
             */
            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("connect");

        // 3、union 联合多条流
//        warningStream.union();
        highTempStream.union(lowTempStream, allTempStream).print("union");

        env.execute();
    }
}
