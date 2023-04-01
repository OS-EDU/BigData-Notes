package com.erxi.apitest.source;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author AKA二夕
 * @create 2021-02-09 上午9:35
 */

/**
 * 从集合中读取数据：
 * ①首先得定义一个类，表示集合，然后类中所含有的属性，这里我们创建SensorReading类
 * 所包含的属性有：id、timestamp、temperature，均定义为私有属性，通过get、set方法返回；
 * ②在获取集合中的数据时，使用flink中报包含的fromCollection方法，可以使用Arrays集合进行存储；
 * ③如果是直接获取数据元素，使用多对应的fromElements方法即可；
 * ④使用print() 方法打印输出；
 * ⑤调用execute() 方法执行程序，这个时候可能会报错，注意main方法抛出异常即可。
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 设置并行度

        // 从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        // 直接读取数据中的元素
        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);

        // 打印输出
        dataStream.print("Collection");
        integerDataStream.print("Element");

        // 执行
        env.execute();

    }
}
