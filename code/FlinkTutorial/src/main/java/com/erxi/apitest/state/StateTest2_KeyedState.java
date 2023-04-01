package com.erxi.apitest.state;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author AKA二夕
 * @create 2021-02-18 16:19
 */
public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket 文本流
//        DataStreamSource<String> inputPath = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> inputPath = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
        DataStream<SensorReading> dataStream = inputPath.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的 map 操作，统计当前 sensor 数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCounterMapper());

        resultStream.print();

        env.execute();
    }

    // 自定义 RichMapFunction
    public static class MyKeyCounterMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> keyCountState;

        // 其它类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-counter", Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>(""));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其它状态 API 调用

            // list state
            for (String str : myListState.get()) {
                System.out.println(str);
            }
            myListState.add("hello");

            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");

            myMapState.clear();

            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
