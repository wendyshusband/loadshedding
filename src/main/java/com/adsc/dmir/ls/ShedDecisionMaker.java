package com.adsc.dmir.ls;

import java.util.ArrayList;

/**
 * Created by kailin on 14/3/17.
 */
public class ShedDecisionMaker implements IShedDecisionMaker {

    public ShedDecisionMaker(){}

    @Override
    public boolean decisionMaker(Object[] arg) {
        if(Integer.valueOf(arg[1].toString()) >= (Integer.valueOf(arg[0].toString())/2)){
            return true;
        }
        return false;
    }

}
