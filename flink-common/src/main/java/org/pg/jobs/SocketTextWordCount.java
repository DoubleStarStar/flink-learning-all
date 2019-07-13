package org.pg.jobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Starstar Sun
 * @date 2019/7/13
 * @Description:
 **/
public class SocketTextWordCount {

    public static void main(String[] args) throws Exception {
        if(args.length!=2){
            throw new IllegalArgumentException("host and port");
        }
        String host=args[0];
        Integer port=Integer.parseInt(args[1]);
        // 1.init env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.get data
        DataStreamSource<String> stream = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute("Java WordCount from SocketTextStream Example!");
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for(String token:tokens){
                collector.collect(new Tuple2<>(token,1));
            }
        }
    }
}
