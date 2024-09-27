package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class ReaderFunction extends KeyedStateReaderFunction<String, KeyedState> {
    private ValueState<Tuple2<String,Long>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<String,Long>> stateDescriptor = new
                ValueStateDescriptor<>("_op_state", TypeInformation.of(new TypeHint<Tuple2<String,Long>>(){}));
        state = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void readKey(String key, Context ctx, Collector<KeyedState> out) throws Exception {
        KeyedState data = new KeyedState();
        data.key = state.value().f0;
        data.value =state.value().f1;
        out.collect(data);
    }
}

