package com.adsc.dmir.ls.bolts;

import com.adsc.dmir.debug.TestPrint;
import com.adsc.dmir.ls.IShedding;
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
    final int tupleQueueCapacity = 10;
    private transient BlockingQueue<Tuple> pendingTupleQueue;

    public LoadsheddingBoltExecutor(WorkBolt bolt, IShedding shedder){
        _shedder = shedder;
        _bolt = bolt;
        //new TestPrint("workbolt=",_bolt.toString());
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
                try {
                    boolean done = false;
                    while (!done){
                        Tuple tuple = pendingTupleQueue.take();
                        drainer.clear();
                        drainer.add(tuple);
                        pendingTupleQueue.drainTo(drainer);
                        new TestPrint("pending_queue_size=",drainer.size());
                        //tupleQueueCapacity/2
                        if(drainer.size() >=5) {
                            ArrayList<Tuple> result = (ArrayList<Tuple>) _shedder.drop(drainer);
                            for(Tuple t : result)
                                _bolt.execute(t);
                            System.out.println("ifdone!!!!!!!");
                        }else{
                            for(Tuple t : drainer)
                                _bolt.execute(t);
                            System.out.println("elsedone!!!!!!!");
                        }
                        Thread.sleep(500);
                    }
                } catch (Exception e){
                    e.getStackTrace();
                }

            }

        });
        thread.start();
        System.out.println("loadshedding thread start!");
    }

    public void execute(Tuple tuple) {
        System.out.println("ssss="+tuple.getValue(0));
        try {
            pendingTupleQueue.put(tuple);
            //pendingTupleQueue.put(tuple);
            //pendingTupleQueue.put(tuple);
            //System.out.println("pendsize="+pendingTupleQueue.size());
            int i= 10000;
            while(i>0){
                i--;
            }
            //handle();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void handle() {
            try {
                Tuple input = pendingTupleQueue.take();
                _bolt.execute(input);
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

