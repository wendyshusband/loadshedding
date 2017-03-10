package com.adsc.dmir.ls.bolts;

import com.adsc.dmir.ls.IShedding;
import com.adsc.dmir.debug.TestPrint;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kailin on 4/3/17.
 */
public class LoadsheddingBoltExecutor implements IRichBolt {

    public static Logger LOG = LoggerFactory.getLogger(LoadsheddingBoltExecutor.class);

    private IRichBolt _bolt;
    private IShedding _shedder;
    private transient OutputCollector _collector;
    final int tupleQueueCapacity = 50;
    private transient BlockingQueue<Tuple> pendingTupleQueue;
    private transient BlockingQueue<Tuple> receiveTupleQueue;
    private transient BlockingQueue<Tuple> dropTupleQueue;

    public LoadsheddingBoltExecutor(WorkBolt bolt, IShedding shedder){
        _shedder = shedder;
        _bolt = bolt;
        new TestPrint("workbolt=",_bolt.toString());
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _bolt.prepare(map,topologyContext,outputCollector);
        _collector = new OutputCollector(outputCollector);
        pendingTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        receiveTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        dropTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        loadsheddingThread();
    }

    /*test*/
    public void prepare(){
        pendingTupleQueue = new ArrayBlockingQueue<Tuple>(tupleQueueCapacity);
        loadsheddingThread();
    }

    private void loadsheddingThread() {

        final Thread thread = new Thread(new Runnable() {
            public void run() {
                ArrayList<Tuple> drainer = new ArrayList<Tuple>();
                try {
                    while (true){
                        //Tuple tuple = pendingTupleQueue.take();
                        //drainer.add(tuple);
                        //new TestPrint("wang?=",pendingTupleQueue.size());
                        pendingTupleQueue.drainTo(drainer, 30);
                        //new TestPrint("chong?=",drainer.size());
                        for(Tuple input : drainer) {
                            boolean isDrop = _shedder.drop(input);
                            //new TestPrint("wwc?=",isDrop);
                            if(isDrop){
                                dropTupleQueue.put(input);
                            }else{
                                receiveTupleQueue.put(input);
                            }

                        }
                        drainer.clear();
                    }

                } catch (InterruptedException e){
                    e.getStackTrace();
                }

            }

        });
        thread.start();
        System.out.println("loadshedding thread start!");
    }

    public void execute(Tuple tuple) {
        try {
            new TestPrint("tup=",tuple.toString());
            pendingTupleQueue.put(tuple);
            new TestPrint("pendingTupleQueuesize=",pendingTupleQueue.size());
            handle();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void handle() {
        try {
            if(!receiveTupleQueue.isEmpty()) {
                _bolt.execute(receiveTupleQueue.take());
                //new TestPrint("1?=",receiveTupleQueue.size());
            }
            if(!dropTupleQueue.isEmpty()) {
                _collector.ack(dropTupleQueue.take());
                //new TestPrint("2?=",dropTupleQueue.size());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

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

    }
}

