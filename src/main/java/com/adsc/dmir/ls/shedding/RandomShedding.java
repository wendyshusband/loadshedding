package com.adsc.dmir.ls.shedding;

import com.adsc.dmir.ls.shedding.IShedding;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by kailin on 6/3/17.
 */
public class RandomShedding implements IShedding,Serializable {
    public RandomShedding(){}

    public List drop(double shedRate, List queue, OutputCollector collector) {
        System.out.println("shedrate="+ shedRate);
        Iterator<Tuple> it = queue.iterator();
        Random random = new Random();
        while(it.hasNext()){
            Tuple t = it.next();

            if(((random.nextInt(10)+1.0) / 10.0) <= shedRate){
                collector.fail(t);
                System.out.println("fail the tuple:"+t.toString());
                it.remove();
            }
        }
        return queue;
    }

}
