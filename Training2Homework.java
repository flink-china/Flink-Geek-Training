/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Lesson 2 Streaming Processing with Apache Flink -- Homework
 *
 * @author xccui
 */
public class Training2Homework {
    private static List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    public static void main(String[] args) throws Exception {
        eventTimeWindow();
    }

    /**
     * Homework: Event time tumbling window with state and timer.
     */
    public static void eventTimeWindow() throws Exception {
        final long windowSize = 1000;

        StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();

        // DON'T FORGET THIS!!!
        e.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // A source with 500ms random delay.
        DataStreamSource<Integer> source = e
                .addSource(new SourceFunction<Integer>() {
                    private volatile boolean stop = false;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        int i = 0;
                        while (!stop && i < data.size()) {
                            long currentTime = System.currentTimeMillis();
                            ctx.collectWithTimestamp(
                                    data.get(i++),
                                    currentTime - random.nextInt(500));
                            // 1. Emit watermark since we use the collectWithTimestamp() to generate timestamps.
                            ctx.emitWatermark(new Watermark(currentTime - 500));
                            Thread.sleep(200);
                        }
                    }
                    @Override
                    public void cancel() {
                        stop = true;
                    }
                }).setParallelism(1);
        source.keyBy(num -> num % 2).process(new KeyedProcessFunction<Integer, Integer, String>() {

            private MapState<Long, String> windows;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2. Use map state
                MapStateDescriptor<Long, String> mapStateDescriptor =
                        new MapStateDescriptor<>("windows", Long.class, String.class);
                windows = getRuntimeContext().getMapState(mapStateDescriptor);
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                Long eventTime = ctx.timestamp();
                if (eventTime < ctx.timerService().currentWatermark()) {
                    // Deal with late data. We simply drop them here.
                } else {
                    // Window start (event) time
                    Long windowStart = eventTime / windowSize * windowSize;
                    if (!windows.contains(windowStart)) {
                        // Create a window and register a fire/cleanup timer for it.
                        windows.put(windowStart, String.valueOf(value));
                        // 3. Register timers
                        ctx.timerService().registerEventTimeTimer(windowStart + windowSize);
                    } else {
                        windows.put(windowStart, windows.get(windowStart) + ", " + value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                // What to do when a registered timer triggers?
                Long windowStart = timestamp - windowSize;
                out.collect(windows.get(windowStart));
                windows.remove(windowStart);
            }
        }).print();
        System.out.println(e.getExecutionPlan());
        e.execute();
    }

}
