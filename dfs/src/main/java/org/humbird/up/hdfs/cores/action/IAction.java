package org.humbird.up.hdfs.cores.action;

/**
 * Created by david on 16/9/10.
 */
public interface IAction<O, S, D> {
    void Play(O object) throws Exception;

    D getDest();

    void setDest(D dest);

    S getSources();

    String getType();

    void setType(String type);
}
