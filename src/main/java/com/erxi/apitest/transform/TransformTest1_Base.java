package com.erxi.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author AKA二夕
 * @create 2021-02-09 11:32
 */

/**
 * 该类为使用基本的“转换算子”的方法：
 * ①map方法用于将数据转换成Integer输出，调用MapFunction<String, Integer>,
 * 其中String表示输入元素的类型，Integer表述返回的类型；
 * ②flatMap方法用于，按照某种特定的规则处理数据，比如这里我们使用的是按照“,”逗号分割，
 * 可以转换数据类型；
 * ③filter方法，通常用于筛选数据，并不能改变数据的类型
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 1、map，把String转换成长度输出
        SingleOutputStreamOperator<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            /**
             * The mapping method. Takes an element from the input data set and transforms
             * it into exactly one element.
             * @param value 输入的数据类型
             * @return 经过转换之后的数据类型，也就是Integer
             * @throws Exception 处理异常
             */
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        // 2、flapMap，按逗号分字段
        SingleOutputStreamOperator<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            /**
             * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
             * it into zero, one, or more elements.
             * @param value 输入的数据类型
             * @param out 收集处理过之后的结果
             * @throws Exception 处理异常情况
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        // 3、filter，筛选 sensor_1 开头的id对应的数据
        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            /**
             * The filter function that evaluates the predicate.
             * @param value 需要被筛选的数据
             * @return 返回经过筛选之后，符合条件的值
             * @throws Exception 抛出异常
             */
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        // 打印输出
        mapStream.print("map");
        flatMapStream.print("flapMap");
        filterStream.print("filter");

        env.execute();

    }
}
