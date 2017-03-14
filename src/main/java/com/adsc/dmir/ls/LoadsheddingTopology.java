package com.adsc.dmir.ls;

import com.adsc.dmir.ls.bolts.RandomSheddableBolt;
import com.adsc.dmir.ls.bolts.WorkBolt;
import com.adsc.dmir.ls.bolts.outputBolt;
import com.adsc.dmir.ls.shedding.RandomShedding;
import com.adsc.dmir.ls.spouts.TestOverLoadSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by kailin on 4/3/17.
 */
public class LoadsheddingTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new TestOverLoadSpout(false), 3);
        builder.setBolt("loadshedding", new RandomSheddableBolt(new WorkBolt(),new RandomShedding()), 2).shuffleGrouping("spout");
        builder.setBolt("output",new outputBolt(),1).shuffleGrouping("loadshedding");
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
        }
    }
}
