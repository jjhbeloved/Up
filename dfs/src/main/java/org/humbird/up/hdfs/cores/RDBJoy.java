package org.humbird.up.hdfs.cores;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.humbird.up.hdfs.commons.Constant;
import org.humbird.up.hdfs.cores.action.IAction;

/**
 * Created by david on 16/9/11.
 */
public class RDBJoy implements IJoy {

    private final static Logger LOGGER = LogManager.getLogger(RDBJoy.class);

    @Override
    public void make(IAction action) throws Exception {
        Session session = null;
        try {
            Constant.hbFactory.openSession();
            action.play(session);
        } finally {
            if (session != null) {
                session.close();
            }
        }

    }

    @Override
    public Object happy(IAction action) throws Exception {
        return null;
    }
}
