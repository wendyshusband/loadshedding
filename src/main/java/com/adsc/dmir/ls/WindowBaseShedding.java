package com.adsc.dmir.ls;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by kailin on 13/3/17.
 */
public class WindowBaseShedding implements IShedding{

    public WindowBaseShedding(){}

    public List<Tuple> drop(double shedRate, List<Tuple> queue, OutputCollector collector) {

        return null;
    }
}
