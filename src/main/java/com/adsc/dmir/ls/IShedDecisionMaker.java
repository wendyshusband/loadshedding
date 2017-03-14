package com.adsc.dmir.ls;

/**
 * Created by kailin on 14/3/17.
 */
public interface IShedDecisionMaker<T> {

    boolean decisionMaker(T ...arg);
}
