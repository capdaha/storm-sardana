package com.sardana;

import com.sardana.bolt.MeasurementStatBolt;
import com.sardana.spout.MeasurementSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author Sardana
 */
public class MeasurementParsingTopology {

    public static void main(String[] args) throws Exception {
        String fileName = args[0];
        if (fileName == null) {
            throw new Exception("The dataset file is not specified.");
        }
        TopologyBuilder builder = new TopologyBuilder();

        // set the number of tasks to one, for the spout reads from one file
        builder.setSpout("measurement-spout", new MeasurementSpout(), 1);
        builder.setBolt("count-bolt", new MeasurementStatBolt(), 2).fieldsGrouping("measurement-spout", new Fields("droneId"));

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.put(MeasurementSpout.FILE_NAME_CONFIG, fileName);
        conf.setDebug(false);
        conf.setMaxTaskParallelism(2);

        StormRunner.runTopologyLocally(topology, "measurement-parsing-topology", conf, 30);
    }
}
