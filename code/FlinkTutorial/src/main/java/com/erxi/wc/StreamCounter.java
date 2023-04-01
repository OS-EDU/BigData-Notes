package com.erxi.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author AKA二夕
 * @create 2021-02-05 下午10:45
 */
public class StreamCounter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the txt file
//        String inputPath = "src/main/resources/hello.txt";
//        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

        // 使用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
//
//        // 从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);

        // deal
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCounter.MyFLapMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();

        env.execute();
    }
}
