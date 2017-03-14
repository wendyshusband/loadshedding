package com.adsc.dmir.ls.bolts;

import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * Created by kailin on 14/3/17.
 */
public class PeekBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println("Spout: fields: " + tuple.getFields().toList() + " values: " + tuple.getValues());
        collector.emit(tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("value"));
    }

}
