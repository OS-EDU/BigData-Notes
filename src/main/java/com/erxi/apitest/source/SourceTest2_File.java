package com.erxi.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author AKA二夕
 * @create 2021-02-09 上午9:50
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取文件路径
        String inputPath = "src/main/resources/sensor.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        dataStream.print();
        env.execute();
    }
}
