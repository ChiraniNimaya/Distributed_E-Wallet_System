package com.ewallet.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock implements Watcher {
    private String childPath;
    private ZooKeeperClient client;
    private String lockPath;
    private boolean isAcquired = false;
    private String watchedNode;
    private byte[] myDataBytes;
    CountDownLatch startFlag = new CountDownLatch(1);
    CountDownLatch eventReceivedFlag;

    public static String zooKeeperUrl;
    private static String lockProcessPath = "/lp_";

    public static void setZooKeeperURL(String url) {
        zooKeeperUrl = url;
    }

    public DistributedLock(String lockName, String data)
            throws IOException, KeeperException, InterruptedException {
        myDataBytes = data.getBytes(StandardCharsets.UTF_8);
        this.lockPath = "/" + lockName;
        client = new ZooKeeperClient(zooKeeperUrl, 5000, this);
        startFlag.await();

        if (client.CheckExists(lockPath) == false) {
            createRootNode();
        }
        createChildNode();
    }

    private void createRootNode() throws InterruptedException, UnsupportedEncodingException, KeeperException {
        lockPath = client.createNode(lockPath, false, CreateMode.PERSISTENT, new byte[0]);
        System.out.println("Root node created at " + lockPath);
    }

    private void createChildNode() throws InterruptedException, UnsupportedEncodingException, KeeperException {
        childPath = client.createNode(lockPath + lockProcessPath, false,
                CreateMode.EPHEMERAL_SEQUENTIAL, myDataBytes);
        System.out.println("Child node created at " + childPath);
    }

    public void acquireLock() throws KeeperException, InterruptedException {
        String smallestNode = findSmallestNodePath();
        if (smallestNode.equals(childPath)) {
            isAcquired = true;
        } else {
            do {
                System.out.println("Lock is currently acquired by node " + smallestNode + " .. hence waiting..");
                eventReceivedFlag = new CountDownLatch(1);
                watchedNode = smallestNode;
                client.addWatch(smallestNode);
                eventReceivedFlag.await();
                smallestNode = findSmallestNodePath();
            } while (!smallestNode.equals(childPath));
            isAcquired = true;
        }
    }

    public boolean tryAcquireLock() throws KeeperException, InterruptedException {
        String smallestNode = findSmallestNodePath();
        if (smallestNode.equals(childPath)) {
            isAcquired = true;
        }
        return isAcquired;
    }

    public void releaseLock() throws KeeperException, InterruptedException {
        if (!isAcquired) {
            throw new IllegalStateException("Lock needs to be acquired first to release");
        }
        client.delete(childPath);
        isAcquired = false;
    }

    public byte[] getLockHolderData() throws KeeperException, InterruptedException {
        String smallestNode = findSmallestNodePath();
        return client.getData(smallestNode, true);
    }

    public List<byte[]> getOthersData() throws KeeperException, InterruptedException {
        List<byte[]> result = new ArrayList<>();
        List<String> childrenNodePaths = client.getChildrenNodePaths(lockPath);
        for (String path : childrenNodePaths) {
            path = lockPath + "/" + path;
            if (!path.equals(childPath)) {
                byte[] data = client.getData(path, false);
                result.add(data);
            }
        }
        return result;
    }

    private String findSmallestNodePath() throws KeeperException, InterruptedException {
        List<String> childrenNodePaths = null;
        childrenNodePaths = client.getChildrenNodePaths(lockPath);
        Collections.sort(childrenNodePaths);
        String smallestPath = childrenNodePaths.get(0);
        smallestPath = lockPath + "/" + smallestPath;
        return smallestPath;
    }

    @Override
    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        Event.EventType type = event.getType();

        if (Event.KeeperState.SyncConnected == state) {
            if (Event.EventType.None == type) {
                System.out.println("Successfully connected to the server");
                startFlag.countDown();
            }
        }

        if (Event.EventType.NodeDeleted.equals(type)) {
            if (watchedNode != null && eventReceivedFlag != null && event.getPath().equals(watchedNode)) {
                System.out.println("NodeDelete event received. Trying to get the lock..");
                eventReceivedFlag.countDown();
            }
        }
    }
}