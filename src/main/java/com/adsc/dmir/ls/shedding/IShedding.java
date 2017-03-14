package com.adsc.dmir.ls.shedding;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by kailin on 6/3/17.
 */
public interface IShedding<E> {
    /**
     * drop operate
     * @param collector
     * @param shedRate
     * @param queue a list of tuple
     */
    List<E> drop(double shedRate, List<E> queue, OutputCollector collector);
}
