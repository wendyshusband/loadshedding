package com.adsc.dmir.ls;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by kailin on 6/3/17.
 */
public class Shedding implements IShedding,Serializable {
    public Shedding(){}

    public boolean drop(Tuple tuple) {
        Random random = new Random();
        int i = random.nextInt(2);
        if(i % 2 == 0)
            return true;
        return false;
    }
}
