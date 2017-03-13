package com.adsc.dmir.ls;


import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by kailin on 6/3/17.
 */
public interface IShedding {
    /**
     * drop operate
     * @param queue a list of tuple
     */
    List<Tuple> drop(List<Tuple> queue);
}
