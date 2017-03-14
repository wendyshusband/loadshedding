package com.adsc.dmir.ls.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

/**
 * Created by kailin on 14/3/17.
 */
public class TumblingWindowAvgBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        int sum = 0;
        List<Tuple> tuplesInWindow = inputWindow.get();
        if (tuplesInWindow.size() > 0) {
                /*
                * Since this is a tumbling window calculation,
                * we use all the tuples in the window to compute the avg.
                */
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
            for (Tuple tuple : tuplesInWindow) {
                System.out.println(" values: " + tuple.getValues());
                sum += (Integer) tuple.getValue(0);
            }
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
            collector.emit(new Values(sum / tuplesInWindow.size()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("avg"));
    }

}
