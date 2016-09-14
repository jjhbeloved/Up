package org.humbird.up.hdfs.cores.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/10.
 */
public class ImportRDBAction implements IAction<Session, List<Object>, Object> {

    private final static Logger LOGGER = LogManager.getLogger(ImportRDBAction.class);

    private List<Object> sources = new ArrayList();

    private Object dest;

    private String type;

    @Override
    public void Play(Session session) throws Exception {
        Transaction transaction = session.beginTransaction();
        if (sources.size() > 0) {
            session.doWork(connection -> {
                PreparedStatement ps = null;
                try {
                    ps = connection.prepareStatement(String.valueOf(dest));
                    for (int i = 0; i < sources.size(); i++) {
                        List<Object> objs = (List<Object>) sources.get(i);
                        try {
                            for (int j = 1; j <= objs.size(); j++) {
                                ps.setObject(j, objs.get(j - 1));
                            }
                            ps.addBatch();
                        } catch (SQLException e) {
                            LOGGER.error(e.getMessage());
                        }
                        if ((i + 1) % 500 == 0) {
                            LOGGER.info("---------  " + (i + 1) + "  ---------");
                            ps.executeBatch();
                        }
                    }
                    ps.executeBatch();
                } finally {
                    if (ps != null) {
                        ps.close();
                    }
                }

            });
        } else {
            session.save(dest);
        }
        transaction.commit();
    }

    @Override
    public Object getDest() {
        return dest;
    }

    @Override
    public void setDest(Object dest) {
        this.dest = dest;
    }

    @Override
    public List<Object> getSources() {
        return sources;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }
}
