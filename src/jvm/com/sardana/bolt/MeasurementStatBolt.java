package com.sardana.bolt;

import com.sardana.object.Measurement;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sardana
 */
public class MeasurementStatBolt extends BaseRichBolt {
    private OutputCollector collector;

    private Map<String, Integer> countMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String droneId = input.getString(0);
        //Measurement measurement = (Measurement) input.getValue(1);

        Integer count = countMap.get(droneId);
        // check if the word is present in the map
        if (count == null) {
            count = 0;
        }
        countMap.put(droneId, ++count);

        // emit the droneId and count
        collector.emit(new Values(droneId, countMap.get(droneId)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("droneId", "count"));
    }
}
