package com.adsc.dmir.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;

public class BaseWindowTopology {

    private static long startTime;

    static String relativeTime() {
        return (System.currentTimeMillis() - startTime) / 1000.0 + "s ";
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("integer", new RegularIntegerSpout(), 1);
        builder.setBolt("peek", new PeekBolt(), 1)
                .shuffleGrouping("integer");
        builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(new Count(30), new Count(10)), 1)
                .shuffleGrouping("peek");
        builder.setBolt("tumblingavg", new TumblingWindowAvgBolt().withTumblingWindow(new Count(3)), 1)
                .shuffleGrouping("slidingsum");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingavg");
        Config conf = new Config();
        // conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            // Utils.sleep(100000);
            //cluster.killTopology("test");
            // cluster.shutdown();
            //觉得打印结果能够说明问题时，自己手动停止程序
        }
    }

    private static class RegularIntegerSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private int id = 0;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(1000);
            collector.emit(new Values(++id));//id作为value发送
        }

        @Override
        public void ack(Object msgId) {

        }

        @Override
        public void fail(Object msgId) {

        }
    }

    private static class PrinterBolt extends BaseBasicBolt {//打印最后的结果

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println("fields: " + tuple.getFields().toList() + " values: " + tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }

    }

    private static class PeekBolt extends BaseBasicBolt {//查看从Spout中发出的tuple
        static boolean begin = false;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println("Spout: fields: " + tuple.getFields().toList() + " values: " + tuple.getValues());
            collector.emit(tuple.getValues());
            if (!begin) {
                startTime = System.currentTimeMillis();
                begin = true;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("value"));
        }

    }

    private static class SlidingWindowSumBolt extends BaseWindowedBolt {

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
            System.out.println("Time: " + relativeTime() + "in SlidingWindowSumBolt");
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
    /*
     *Computes tumbling window average
     */
    private static class TumblingWindowAvgBolt extends BaseWindowedBolt {
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
                System.out.println("Time: " + relativeTime() + "in TumblingWindowAvgBolt");
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
}