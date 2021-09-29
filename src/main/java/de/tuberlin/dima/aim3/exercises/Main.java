package de.tuberlin.dima.aim3.exercises;
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


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text
 * files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>write a simple Flink Streaming program,
 *   <li>use tuple data types,
 *   <li>write and use user-defined functions.
 * </ul>
 * Edited for Cloud Computing Group 13, WiSe 2020, Anik Jacobsen, Mark Sumegi, Robin van de Water
 */
public class Main {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);



        // Set up flink execution environment and start web environment on port 8081, only useful when executed locally
//        Configuration conf = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // make parameters available in the web interface

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // Parse input file
        var input = args[1];
        input = input.replaceAll("^\"|\"$", "");
        var text = env.readTextFile(input);

        // Executing word count
        var counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.map(a -> a.toLowerCase())
                        .map(a -> a.replaceAll("/^[A-Za-z]+$/", ""))
                        // Converting all letters to lowercase and removing non-alphabetic characters
                        .flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(value -> value.f0)
                        .sum(1);

        // Collecting the words and their count
        var wordlist = counts.executeAndCollect();
        var words = new ArrayList<Tuple2<String, Integer>>();
        var dictionary = new ArrayList<String>();

        // Putting it into a java arraylist
        while(wordlist.hasNext())
        {
            words.add(wordlist.next());

        }
        // Reversing order
        Collections.reverse(words);

        // Looking for unique words
        var result = new ArrayList<Tuple2<String, Integer>>();
        int counter = 0;
        for (var item:words) {
            if(!dictionary.contains(item.f0))
            {
                result.add(item);
                dictionary.add(item.f0);
            }
            counter++;
            if(counter%1000==0)
            {
                System.out.println(counter);
            }
        }
        //Sorting words, reversing collection
        result.sort((v1, v2) -> v1.f1.compareTo(v2.f1));
        Collections.reverse(result);
        // Printing words
        for (var inval :result) {
            System.out.println(inval);
        }

        //Writing words to file
        if (params.has("output")) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(args[3]));
            writer.write("");
            for (var line: result) {
                writer.append(line.f0).append(",").append(String.valueOf(line.f1));
                writer.append("\n");
            }
            writer.close();
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
        }
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}