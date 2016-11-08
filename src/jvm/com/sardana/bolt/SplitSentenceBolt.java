package com.sardana.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector _collector;
    public static String delims = "[ .,?!]+";

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // get the column word from tuple
        String sentence = input.getString(0);

        // provide the delimiters for splitting the tweet

        // now split the tweet into tokens
        String[] words = sentence.split(delims);

        Arrays.asList(words).forEach(w -> _collector.emit(new Values(w)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
