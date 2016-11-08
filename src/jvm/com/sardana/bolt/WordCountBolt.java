package com.sardana.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector _collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        // get the word from the 1st column of incoming tuple
        String word = input.getString(0);

        Integer count = countMap.get(word);
        // check if the word is present in the map
        if (count == null) {
            count = 0;
        }
        countMap.put(word, ++count);

        // emit the word and count
        _collector.emit(new Values(word, countMap.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
