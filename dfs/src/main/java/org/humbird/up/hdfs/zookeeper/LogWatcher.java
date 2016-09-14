package org.humbird.up.hdfs.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by david on 16/9/8.
 */
public class LogWatcher implements Watcher {

    private final static Logger LOGGER = LogManager.getLogger(LogWatcher.class);

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(watchedEvent.getPath() + ", " + watchedEvent.getState().name() + ", " + watchedEvent.getType());
        }
    }
}
