package com.sardana.spout;

import com.sardana.object.Measurement;
import com.sardana.util.JsonParser;
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
    private SpoutOutputCollector collector;
    private final String fileName = "C:/Java/apache-storm-1.0.2/examples/storm-sardana/src/main/resources/test-dataset.json";
    private List<Measurement> measurements;
    private int index = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
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
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            // TODO: Change to logger
            e.printStackTrace();
        }
        return measurements;
    }
}
