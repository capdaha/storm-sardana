package com.sardana;

import com.sardana.bolt.SplitSentenceBolt;
import com.sardana.bolt.WordCountBolt;
import com.sardana.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split-sentence-bolt", new SplitSentenceBolt(), 2).shuffleGrouping("sentence-spout");
        builder.setBolt("word-count-bolt", new WordCountBolt(), 4).fieldsGrouping("split-sentence-bolt", new Fields("word"));

        StormTopology topology = builder.createTopology();

        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            // run it in a live cluster
            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);
            // submit a topology with config
            StormRunner.runTopologyRemotely(topology, args[0], conf);
        } else {
            // run it in a simulated local cluster
            // set the number of threads to run - similar to setting number of workers in live cluster
            conf.setMaxTaskParallelism(3);
            // create the local cluster instance
            StormRunner.runTopologyLocally(topology, "word-count-topology", conf, 30);
        }
    }
}
