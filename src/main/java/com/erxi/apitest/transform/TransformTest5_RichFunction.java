package com.erxi.apitest.transform;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都有其 Rich 版本。
 * 它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
 */

/**
 * @author AKA二夕
 * @create 2021-02-10 20:13
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成 SensorReading 类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());
        resultStream.print();

        env.execute();

    }

    // 自定义类
    public static class MyMapperTest implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 实现自定义富函数类
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {
        /**
         * 重载 map 函数，
         * getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函数执行的并行度，任务的名字，以及state状态
         * @param value 传入的数据
         * @return 利用二元组返回 sensor_id 和 并行子任务的索引
         * @throws Exception 抛出异常
         */
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
//            getRuntimeContext().getState();
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        /**
         * open() 方法是 rich function 的初始化方法，
         * 当一个算子例如 map 或者 filter 被调用之前 open() 会被调用。
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        /**
         * close() 方法是生命周期中的最后一个调用的方法，做一些清理工作
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            // 一般是关闭连接或清空状态的收尾操作
            System.out.println("close");
        }
    }
}
