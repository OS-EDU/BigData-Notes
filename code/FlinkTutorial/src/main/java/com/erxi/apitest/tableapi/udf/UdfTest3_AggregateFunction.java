package com.erxi.apitest.tableapi.udf;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author AKA二夕
 * @create 2021-02-26 16:07
 */
public class UdfTest3_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1、读取数据
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 2、转换成 POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3、将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 4、自定义聚合函数，求当前传感器的平均温度值
        // 4.1 table API
        AvgTemp avgTemp = new AvgTemp();

        // 需要在环境中注册 UDF
        tableEnv.registerFunction("avgTemp", avgTemp);
        Table resultTable = sensorTable
                .groupBy("id")
                .aggregate("avgTemp(temp) as avgtemp")
                .select("id, avgtemp");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, avgTemp(temp) " +
                " from sensor group by id");

        // 打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的 AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个 accumulate 方法，来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }

}
