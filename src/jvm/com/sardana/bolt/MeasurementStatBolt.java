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
    private static final int MILLIS_IN_SEC = 1000;

    private OutputCollector collector;

    private Map<String, Map<Integer, Integer>> countMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String droneId = input.getString(0);
        Measurement measurement = (Measurement) input.getValue(1);
        Long timeStamp = measurement.getMeasurementTimestamp();

        int second = (int) Long.divideUnsigned(timeStamp, MILLIS_IN_SEC);

        Map<Integer, Integer> droneMap = countMap.get(droneId);

        // check if the drone is present in the map
        if (droneMap == null) {
            droneMap = new HashMap<>();
        }

        Integer count = droneMap.get(second);
        if (count == null) {
            count = 0;
        }
        droneMap.put(second, ++count);
        countMap.put(droneId, droneMap);

        // emit the droneId and count
        collector.emit(new Values(droneId, second, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("droneId", "second", "count"));
    }
}
