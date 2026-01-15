package com.ewallet.lock;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperClient {
    private ZooKeeper zooKeeper;

    public ZooKeeperClient(String zooKeeperUrl, int sessionTimeout, Watcher watcher) throws IOException {
        zooKeeper = new ZooKeeper(zooKeeperUrl, sessionTimeout, watcher);
    }

    public String createNode(String path, boolean shouldWatch, CreateMode mode, byte[] data)
            throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String createdPath = zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        return createdPath;
    }

    public boolean CheckExists(String path) throws KeeperException, InterruptedException {
        Stat nodeStat = zooKeeper.exists(path, false);
        return (nodeStat != null);
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        zooKeeper.delete(path, -1);
    }

    public List<String> getChildrenNodePaths(String root) throws KeeperException, InterruptedException {
        zooKeeper.getState().toString();
        return zooKeeper.getChildren(root, false);
    }

    public void addWatch(String path) throws KeeperException, InterruptedException {
        zooKeeper.exists(path, true);
    }

    public byte[] getData(String path, boolean shouldWatch) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, shouldWatch, null);
    }

    public void write(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, -1);
    }

    public void forceDelete(String path) throws KeeperException, InterruptedException {
        ZKUtil.deleteRecursive(zooKeeper, path);
    }

}