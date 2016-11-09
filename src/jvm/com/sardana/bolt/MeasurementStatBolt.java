package com.sardana.bolt;

import com.sardana.object.Measurement;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Sardana
 */
public class MeasurementStatBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(MeasurementStatBolt.class);

    private OutputCollector collector;
    private Map<String, Map<Long, Integer>> countMap;

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

        long second = TimeUnit.MILLISECONDS.toSeconds(timeStamp);
        Map<Long, Integer> droneMap = countMap.get(droneId);

        // check if the drone is present in the map
        if (droneMap == null) {
            droneMap = new HashMap<>();
            countMap.put(droneId, droneMap);
        }

        Integer count = droneMap.get(second);
        if (count == null) {
            count = 0;
        }
        droneMap.put(second, ++count);

        // emit the droneId and count per second
        collector.emit(new Values(droneId, second, count));
        LOGGER.info(new StringBuilder("New count per second for drone with id: ").append(droneId)
                .append(", second: ").append(second).append(", count: ").append(count).toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("droneId", "second", "count"));
    }
}
