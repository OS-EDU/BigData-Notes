package com.erxi.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author AKA二夕
 * @create 2021-02-05 下午10:13
 */
public class WordCounter {
    public static void main(String[] args) throws Exception {
        // Creating a Stream Processing Execution Environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Reading data from a file
        String inputPath = "src/main/resources/hello.txt";
        DataSource<String> inputDataSet = env.readTextFile(inputPath);

        // The data set is processed, expanded by space binning, and converted into a (word, 1) binary group for statistics
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFLapMapper())
                .groupBy(0)// Grouped according to the word in the first position
                .sum(1);// Sum the data in the second position
        resultSet.print();
    }

    // Custom class that implements the FlatMapFunction interface
    public static class MyFLapMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // Splitting words by space
            String[] words = value.split(" ");
            // Iterate through all the words and output them as a binary
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
