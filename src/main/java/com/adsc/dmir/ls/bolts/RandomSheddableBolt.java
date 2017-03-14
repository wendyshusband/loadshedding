package com.adsc.dmir.ls.bolts;

import com.adsc.dmir.debug.TestPrint;
import com.adsc.dmir.ls.ShedDecisionMaker;
import com.adsc.dmir.ls.shedding.IShedding;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kailin on 4/3/17.
 */
public class RandomSheddableBolt implements IRichBolt,IShedding {

    public static Logger LOG = LoggerFactory.getLogger(RandomSheddableBolt.class);

    private IRichBolt _bolt;
    private IShedding _shedder;
    private transient OutputCollector _collector;
    final int tupleQueueCapacity = 10;
    private transient BlockingQueue<Tuple> pendingTupleQueue;

    public RandomSheddableBolt(WorkBolt bolt, IShedding shedder){
        _shedder = shedder;
        _bolt = bolt;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _bolt.prepare(map,topologyContext,outputCollector);
        _collector = new OutputCollector(outputCollector);
        pendingTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        loadsheddingThread();
    }


    private void loadsheddingThread() {

        final Thread thread = new Thread(new Runnable() {
            public void run() {
                ArrayList<Tuple> drainer = new ArrayList<Tuple>();
                boolean done = false;
                double shedRate = 0.0;
                Tuple tuple = null;
                ShedDecisionMaker decisionMaker = new ShedDecisionMaker();
                Integer[] decision = new Integer[2];
                while (!done) {
                    try {
                        tuple = pendingTupleQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    drainer.clear();
                    drainer.add(tuple);
                    pendingTupleQueue.drainTo(drainer);
                    new TestPrint("pending_queue_size=", drainer.size());
                    shedRate = (drainer.size() * 1.0) / tupleQueueCapacity;
                    decision[0] = tupleQueueCapacity;
                    decision[1] = drainer.size();
                    if (decisionMaker.decisionMaker(decision)) {
                        ArrayList<Tuple> result = (ArrayList<Tuple>) _shedder.drop(shedRate, drainer, _collector);
                        for (Tuple t : result)
                            _bolt.execute(t);
                        System.out.println("ifdone!!!!!!!");
                    } else {
                        for (Tuple t : drainer)
                            _bolt.execute(t);
                        System.out.println("elsedone!!!!!!!");
                    }
                }
            }

        });
        thread.start();
        LOG.info("loadshedding thread start!");
    }

    public void execute(Tuple tuple) {
        System.out.println("tuple="+tuple.getValue(0));
        try {
            pendingTupleQueue.put(tuple);
            //handle();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

 /*   private void handle() {
            try {
                Tuple input = pendingTupleQueue.take();
                _bolt.execute(input);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }*/

    public void cleanup() {
        _bolt.cleanup();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        _bolt.declareOutputFields(outputFieldsDeclarer);
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public static void main(String[] args) {
        //System.out.println(System.currentTimeMillis());
    }

    @Override
    public List drop(double shedRate, List queue, OutputCollector collector) {
        return null;
    }
}

