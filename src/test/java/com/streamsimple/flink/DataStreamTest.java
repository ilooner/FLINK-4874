package com.streamsimple.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * Example taken from https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html
 */
public class DataStreamTest
{
  @Test
  public void simpleTest() throws Exception
  {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setParallelism(1);

    DataStream<Tuple2<String, Integer>> dataStream = env
        .socketTextStream("localhost", 9999)
        .flatMap(new Splitter())
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1);

    dataStream.print();

    env.execute("Window WordCount");
  }

  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>>
  {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception
    {
      for (String word : sentence.split(" ")) {
        out.collect(new Tuple2<String, Integer>(word, 1));
      }
    }
  }
}
