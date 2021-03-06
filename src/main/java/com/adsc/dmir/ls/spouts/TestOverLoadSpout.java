package com.adsc.dmir.ls.spouts;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by kailin on 4/3/17.
 */
public class TestOverLoadSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestOverLoadSpout.class);

    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public TestOverLoadSpout() {
        this(true);
    }

    public TestOverLoadSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    public void nextTuple() {
        Utils.sleep(100);
        final Random random = new Random();
        _collector.emit(new Values(String.valueOf(random.nextInt(1000))));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("resource"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed){
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        }else{
            return null;
        }

    }
}
