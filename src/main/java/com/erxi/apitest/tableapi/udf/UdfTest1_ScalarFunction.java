package com.erxi.apitest.tableapi.udf;

import com.erxi.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author AKA二夕
 * @create 2021-02-26 10:22
 */
public class UdfTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1、读取数据
        DataStream<String> inputPath = env.readTextFile("src/main/resources/sensor.txt");

        // 2、转换成 POJO
        DataStream<SensorReading> dataStream = inputPath.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3、将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 4、自定义标量函数，实现求 id 的 hash 值
        // 4.1 table API
        HashCode hashCode = new HashCode(23);
        // 需要在环境中注册 UDF
        tableEnv.registerFunction("hashcode", hashCode);
        Table resultTable = sensorTable.select("id, ts, hashCode(id)");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor");


        // 打印输出
        resultTable.printSchema();
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的 ScalarFunction
    public static class HashCode extends ScalarFunction {
        private int factor = 1;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
            return str.hashCode() * factor;
        }
    }
}
