package org.humbird.up.hdfs.cores;

import org.humbird.up.hdfs.cores.action.IAction;

/**
 * Created by david on 16/9/10.
 */
public interface IJoy {

    void make(IAction action) throws Exception;

}
