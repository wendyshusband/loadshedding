package com.adsc.dmir.ls;

import com.adsc.dmir.ls.bolts.*;
import com.adsc.dmir.ls.shedding.RandomShedding;
import com.adsc.dmir.ls.spouts.RegularIntegerSpout;
import com.adsc.dmir.ls.spouts.TestOverLoadSpout;
import com.adsc.dmir.test.BaseWindowTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

/**
 * Created by kailin on 14/3/17.
 */
public class WindowedSheddingTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("integer", new RegularIntegerSpout(), 1);
        builder.setBolt("peek", new PeekBolt(), 1)
                .shuffleGrouping("integer");
        builder.setBolt("sliding", new SlidingWindowSumBolt().withWindow(new BaseWindowedBolt.Count(6), new BaseWindowedBolt.Count(2)), 1)
                .shuffleGrouping("peek");
        builder.setBolt("tumbling", new TumblingWindowAvgBolt().withTumblingWindow(new BaseWindowedBolt.Count(3)), 1)
                .shuffleGrouping("sliding");
        builder.setBolt("printer", new outputBolt(), 1).shuffleGrouping("tumbling");
        Config conf = new Config();
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
        }
    }
}
