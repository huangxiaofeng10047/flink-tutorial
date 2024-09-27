package com.yzhou.blog.wordcount.checkpoint;

import java.io.IOException;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ReadCheckpointData {
 public static void main(String[] args) throws Exception {
     String metadataPath="/home/xfhuang/workspace/bigdata/src/flink/cppath/e9f4c927c7e13174f2181c2ac7aea568/chk-2";
     StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
     SavepointReader savepointReader=SavepointReader.read(
         env,
         metadataPath,
         new HashMapStateBackend()
     );
     DataStream<KeyedState> keyedStateDataStream=savepointReader.readKeyedState(OperatorIdentifier.forUid("wc-sum"),new ReaderFunction());
//     keyedStateDataStream.print();
     keyedStateDataStream.addSink(new SinkFunction<KeyedState>() {
         @Override
         public void invoke(KeyedState value, Context context) throws Exception {
//             SinkFunction.super.invoke(value, context);
             System.out.println(value.key+":"+value.value);
         }

     });
     env.execute();
     System.out.println("hello flink");
 }
}
