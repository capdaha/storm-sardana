package com.sardana.spout;

import com.sardana.object.Measurement;
import com.sardana.util.JsonParser;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * @author Sardana
 */
public class MeasurementSpout extends BaseRichSpout {
    public static final String FILE_NAME_CONFIG = "measurement_spout_filename";
    private static final Logger LOGGER = Logger.getLogger(MeasurementSpout.class);

    private SpoutOutputCollector collector;
    private List<Measurement> measurements;
    private int index = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        String fileName = (String) conf.get(FILE_NAME_CONFIG);
        measurements = readMeasurements(fileName);
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        if (index >= measurements.size()) {
            index = index % measurements.size();
        }
        Measurement m = measurements.get(index++);
        collector.emit(new Values(m.getValue().getDroneId(), m));
        LOGGER.info("New measurement for drone with id: " + m.getValue().getDroneId());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("droneId", "measurement"));
    }

    private List<Measurement> readMeasurements(String fileName) {
        List<Measurement> measurements = new ArrayList<>();
        JsonParser parser = new JsonParser(Measurement.class);

        File file = new File(fileName);
        try {
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                try {
                    Measurement m = (Measurement) parser.parseJson(line);
                    measurements.add(m);
                } catch (IOException e) {
                    LOGGER.error("Error in parsing JSON: " + e.getMessage());
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("Error in reading file: " + e.getMessage());
        }
        return measurements;
    }
}
