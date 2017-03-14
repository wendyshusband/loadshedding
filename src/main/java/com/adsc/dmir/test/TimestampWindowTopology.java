package com.adsc.dmir.test;

/**
 * Created by kailin on 14/3/17.
 */
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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TimestampWindowTopology {

    private static long startTime;
    private static SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("timestamp", new TimestampSpout(), 1);
        builder.setBolt("peek", new PeekBolt(), 1)
                .shuffleGrouping("timestamp");

        //WindowBolt参数：Window length = 20s, sliding interval = 10s, watermark emit frequency = 1s, max lag = 5s
        BaseWindowedBolt windowBolt = new WatchSlidingWindowBolt()
                .withWindow(new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS))
                .withTimestampField("timestamp")
                .withWatermarkInterval(new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS))
                .withLag(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));

        builder.setBolt("slidingsum", windowBolt, 1)
                .shuffleGrouping("peek");
        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            // Utils.sleep(100000);
            //cluster.killTopology("test");
            // cluster.shutdown();
            // 觉得打印结果能够说明问题时，自己手动停止程序
        }
    }

    private static String relativeTime() {
        return (System.currentTimeMillis() - startTime) / 1000.0 + "s ";
    }

    private static class TimestampSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private int id = 0;
        private long timestamp;
        private int hour=0;
        private boolean needToChange=false;//标记是否该修改当前小时

        //自定义时间戳的秒
        private int[] seconds = new int[]{4, 5, 7, 18, 26, 34, 35, 47, 48, 56
        };

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("timestamp", "id", "value"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);//一秒钟10个tuple
            int loopIndex=id % seconds.length;
            int sec = seconds[loopIndex];//秒循环用自定义数组的值
            if(loopIndex==0){
                hour=(hour+1)%24;//每当秒用完一次循环，小时加一
            }
            if(loopIndex==seconds.length/2&&id>seconds.length){
                //每一秒发射的中间一个tuple故意设置为“迟到的”,但是第一次产生水印时还没有旧的水印，故加上id>seconds.length排除第一次的“迟到”tuple
                needToChange=true;
                hour--;//将时间戳提前1小时，来产生“迟到的”tuple
            }
            timestamp = getTimestamp(hour, sec);
            Date date = new Date(timestamp);
            String value = df.format(date);
            collector.emit(new Values(timestamp, id++, value));
            if(needToChange){//发射完之后将小时改回来
                needToChange=false;
                hour++;
            }
        }

        private long getTimestamp(int hour, int second) {
            Calendar now = Calendar.getInstance();
            now.set(Calendar.HOUR_OF_DAY, hour);
            now.set(Calendar.MINUTE, 0);
            now.set(Calendar.SECOND, second);
            return now.getTimeInMillis();
        }

        @Override
        public void ack(Object msgId) {
        }

        @Override
        public void fail(Object msgId) {
        }
    }

    private static class PeekBolt extends BaseBasicBolt {
        static boolean begin = false;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println("Spout: fields: " + tuple.getFields().toList() + " values: " + tupleToString(tuple));
            collector.emit(tuple.getValues());
            if (!begin) {
                startTime = System.currentTimeMillis();
                begin = true;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("timestamp", "id", "value"));
        }

    }


    private static class WatchSlidingWindowBolt extends BaseWindowedBolt {

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            List<Tuple> tuplesInWindow = inputWindow.get();
            List<Tuple> newTuples = inputWindow.getNew();
            List<Tuple> expiredTuples = inputWindow.getExpired();

            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
            System.out.println("Time: " + relativeTime() + "in WatchSlidingWindowBolt");
            System.out.println("tuplesInWindow: " + tuplesToString(tuplesInWindow));
            System.out.println("newTuples: " + tuplesToString(newTuples));
            System.out.println("expiredTuples: " + tuplesToString(expiredTuples));
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    private static String tuplesToString(List<Tuple> tuples) {
        List<String> ret = new ArrayList<String>();
        for (Tuple t : tuples) {
            ret.add(tupleToString(t));
        }
        return ret.toString();
    }

    private static String tupleToString(Tuple t) {
        Date date = new Date(t.getLong(0));
        String timestamp = df.format(date);
        int id = t.getInteger(1);
        String ret = "[timestamp: " + timestamp + ", id: " + id + ",value: " + t.getString(2) + "]";
        return ret;
    }
}