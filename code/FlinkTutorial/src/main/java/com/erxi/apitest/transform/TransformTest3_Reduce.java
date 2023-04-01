package com.erxi.apitest.transform;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @TransformTest3_Reduce 该类用于统计各个 sensor_id 对应温度值的最大值，
 * 注：如果之前扫描到 第一次 sensor_6 为 15.4，第二次为 37.1，第三次扫描为 23.22，则最终输出的为 37.1，即输出最高温度
 * <p>
 * KeyedStream → DataStream: 一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，
 * 返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
 */

/**
 * @author AKA二夕
 * @create 2021-02-09 16:59
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            /**
             *
             * @param value1 第一个需要合并的值
             * @param value2 第二个需要合并的值
             * @return 两个值经过聚合之后的结果，这里为返回当前的温度的最大值
             * @throws Exception 抛出异常
             */
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        /**
         * 按照 sensor_id 进行分组之后，进行聚合统计，
         * CurState 为循环比较的状态， newData 为当前最新的状态
         * 返回：当前传感器的 id， 当前时间的时间戳，传感器温度的最大值
         */
        keyedStream.reduce((curState, newData) -> {
            return new SensorReading(newData.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature()));
        });

        resultStream.print();
        env.execute();
    }
}
