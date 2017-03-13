package com.adsc.dmir.ls;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kailin on 6/3/17.
 */
public class Shedding implements IShedding,Serializable {
    public Shedding(){}

    public List<Tuple> drop(List<Tuple> queue) {
        Iterator<Tuple> it = queue.iterator();
        while(it.hasNext()){
            Tuple t = it.next();
            if(0 ==  Integer.valueOf((String) t.getValue(0)) % 2) {
                it.remove();
            }
        }
        return queue;
    }
}
