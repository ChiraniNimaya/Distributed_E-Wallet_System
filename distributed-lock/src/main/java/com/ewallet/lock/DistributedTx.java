package com.ewallet.lock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public abstract class DistributedTx implements Watcher {
    public static final String VOTE_COMMIT = "vote_commit";
    public static final String VOTE_ABORT = "vote_abort";
    public static final String GLOBAL_COMMIT = "global_commit";
    public static final String GLOBAL_ABORT = "global_abort";

    static String zooKeeperUrl;
    String currentTransaction;
    ZooKeeperClient client;
    DistributedTxListener listener;

    public static void setZooKeeperURL(String url) {
        zooKeeperUrl = url;
    }

    public DistributedTx(DistributedTxListener listener) {
        this.listener = listener;
    }

    public void start(String transactionId, String participantId) throws IOException {
        client = new ZooKeeperClient(zooKeeperUrl, 5000, this);
        onStartTransaction(transactionId, participantId);
    }

    abstract void onStartTransaction(String transactionId, String participantId);

    @Override
    public void process(WatchedEvent watchedEvent) {
    }
}