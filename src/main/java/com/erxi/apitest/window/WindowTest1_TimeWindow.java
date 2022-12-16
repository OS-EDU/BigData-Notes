package com.erxi.apitest.window;

import com.erxi.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author AKA二夕
 * @create 2021-02-12 14:44
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        // 创建执行环境，并设置并行度为 1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成 SensorReading 类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试
        // 1. 增量聚合函数，用于统计某固定时间段内输入数据的个数，每条数据到来就进行计算，保持一个简单的状态。
        DataStream<Integer> resultStream1 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15)) // 开启一个 15s 的窗口
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /** 将给定的值输入到累加器中，并返回经过计算之后得出的结果
                     *
                     * @param value 需要增加的值
                     * @param accumulator 用于增加的值
                     * @return 每输入一个 value ，则 accumulator + 1，并返回结果
                     */
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    /**
                     * 获取经过累加器增量聚合之后的结果
                     * @param accumulator 聚合的累加器
                     * @return 最终聚合的结果
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // 2、全窗口函数：先把窗口所有数据收集起来，等到计算的时候会遍历所有数据。
        DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    /**
                     *
                     * @param tuple 当前的 key
                     * @param window 当前的窗口类型
                     * @param input 输入
                     * @param out 输出
                     * @throws Exception 抛出异常
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = window.getEnd(); // 此窗口的独占结束时间戳
                        Integer count = IteratorUtils.toList(input.iterator()).size(); // 获取当前 sensor 的个数
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

        // 3、其它可选 API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1)) // 允许1分钟内的迟到数据<=比如数据产生时间在窗口范围内，但是要处理的时候已经超过窗口时间了
                .sideOutputLateData(outputTag) // 侧输出流，迟到超过1分钟的数据，收集于此
                .sum("temperature"); // 侧输出流 对 温度信息 求和
        // 之后可以再用别的程序，把侧输出流的信息和前面窗口的信息聚合。（可以把侧输出流理解为用来批处理来补救处理超时数据）

        sumStream.getSideOutput(outputTag).print("late");

        resultStream2.print("window");

        env.execute();
    }
}
