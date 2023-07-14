/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** A {@link StreamOperator} for executing {@link MapFunction MapFunctions}. */
@Internal
public class StreamMap<IN, OUT> extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    public StreamMap(MapFunction<IN, OUT> mapper) {
        // 往下追
        super(mapper);
        // chaining-operator 策略
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // 输出到 output
        // 如果当前 Map 算子下游能 chain 算子 比如 flatmap 则调用 ChainedFlatMapDriver
        // 最终还是调用 RecordWriterOutput.collect() 将处理的数据发送给下游
        output.collect(
                // 用户 Map 算子逻辑处理
                element.replace(userFunction.map(element.getValue()))
        );
    }
}
