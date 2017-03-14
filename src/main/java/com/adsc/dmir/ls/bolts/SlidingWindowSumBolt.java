package com.adsc.dmir.ls.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by kailin on 14/3/17.
 */
public class SlidingWindowSumBolt extends BaseWindowedBolt {
    private int sum = 0;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
            /*
             * The inputWindow gives a view of
             * (a) all the events in the window
             * (b) events that expired since last activation of the window
             * (c) events that newly arrived since last activation of the window
             */
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

            /*
             * Instead of iterating over all the tuples in the window to compute
             * the sum, the values for the new events are added and old events are
             * subtracted. Similar optimizations might be possible in other
             * windowing computations.
             */
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("tuplesInWindow: " + tupleToString(tuplesInWindow));
        System.out.println("newTuples: " + tupleToString(newTuples));
        System.out.println("expiredTuples: " + tupleToString(expiredTuples));
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
        for (Tuple tuple : newTuples) {
            sum += (Integer) tuple.getValue(0);
        }
        for (Tuple tuple : expiredTuples) {
            sum -= (Integer) tuple.getValue(0);
        }
        collector.emit(new Values(sum));
    }

    static List<String> tupleToString(List<Tuple> tuples) {
        List<String> ret = new ArrayList<String>();
        for (Tuple t : tuples) {
            ret.add(t.getValues().toString());
        }
        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }

}
