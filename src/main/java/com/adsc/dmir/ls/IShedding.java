package com.adsc.dmir.ls;


import org.apache.storm.tuple.Tuple;

/**
 * Created by kailin on 6/3/17.
 */
public interface IShedding {
    /**
     * drop operate
     * @param tuple a list of tuple
     */
    public boolean drop(Tuple tuple);
}
