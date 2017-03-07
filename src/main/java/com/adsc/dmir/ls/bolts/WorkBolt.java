package com.adsc.dmir.ls.bolts;

import com.debug.TestPrint;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kailin on 4/3/17.
 */
public class WorkBolt implements IRichBolt {
    OutputCollector _collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        new TestPrint("caonima=",tuple.toString());
        _collector.emit(tuple,new Values(tuple.getString(0)+"tail"));
        _collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}