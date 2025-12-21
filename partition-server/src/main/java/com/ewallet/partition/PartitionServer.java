package com.ewallet.partition;

import com.ewallet.lock.DistributedLock;
import com.ewallet.nameservice.NameServiceClient;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionServer {
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    public static final String ZOOKEEPER_URL = "localhost:2181";

    private final String partitionId;
    private final String host;
    private final int port;
    private final int partitionIndex;

    // Account storage
    private final Map<String, Double> accounts = new ConcurrentHashMap<>();
    private final ReadWriteLock accountLock = new ReentrantReadWriteLock();

    // Transaction management
    private final Map<String, TransactionState> pendingTransactions = new ConcurrentHashMap<>();

    // Leader election
    private DistributedLock leaderLock;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private byte[] leaderData;

    public PartitionServer(String partitionId, String host, int port, int partitionIndex)
            throws IOException, KeeperException, InterruptedException {
        this.partitionId = partitionId;
        this.host = host;
        this.port = port;
        this.partitionIndex = partitionIndex;

        DistributedLock.setZooKeeperURL(ZOOKEEPER_URL);
        String lockName = "EWalletPartition_" + partitionId;
        this.leaderLock = new DistributedLock(lockName, buildServerData(host, port));
    }

    public static String buildServerData(String IP, int port) {
        return IP + ":" + port;
    }

    public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(port)
                .addService(new AccountServiceImpl(this))
                .addService(new TransferServiceImpl(this))
                .build();

        server.start();
        System.out.println("Partition Server [" + partitionId + "] started and ready on port " + port);

        // Register with name service
        registerWithNameService();

        // Start leader election
        tryToBeLeader();

        server.awaitTermination();
    }

    private void registerWithNameService() throws IOException {
        try {
            NameServiceClient client = new NameServiceClient(NAME_SERVICE_ADDRESS);
            String serviceName = "partition_" + partitionId + "_replica_" + port;
            client.registerService(serviceName, host, port, "grpc");
            System.out.println("Registered with name service: " + serviceName);
        } catch (Exception e) {
            System.err.println("Failed to register with name service: " + e.getMessage());
        }
    }

    private void tryToBeLeader() {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            System.out.println("Starting leader campaign for partition " + partitionId);
            try {
                boolean leader = leaderLock.tryAcquireLock();
                while (!leader) {
                    byte[] newLeaderData = leaderLock.getLockHolderData();
                    if (currentLeaderData != newLeaderData) {
                        currentLeaderData = newLeaderData;
                        setCurrentLeaderData(currentLeaderData);
                        System.out.println("New leader detected: " + new String(currentLeaderData));
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
                System.out.println("I am now the leader for partition " + partitionId);
                isLeader.set(true);
                currentLeaderData = null;
            } catch (Exception e) {
                System.err.println("Error in leader campaign: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    public synchronized String[] getCurrentLeaderData() {
        if (leaderData == null) return null;
        return new String(leaderData).split(":");
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    public String getPartitionId() {
        return partitionId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    // Account operations
    public boolean createAccount(String accountId, double initialBalance) {
        accountLock.writeLock().lock();
        try {
            if (accounts.containsKey(accountId)) {
                return false;
            }
            accounts.put(accountId, initialBalance);
            System.out.println("Account created: " + accountId + " with balance: " + initialBalance);
            return true;
        } finally {
            accountLock.writeLock().unlock();
        }
    }

    public Double getBalance(String accountId) {
        accountLock.readLock().lock();
        try {
            return accounts.get(accountId);
        } finally {
            accountLock.readLock().unlock();
        }
    }

    public boolean hasAccount(String accountId) {
        accountLock.readLock().lock();
        try {
            return accounts.containsKey(accountId);
        } finally {
            accountLock.readLock().unlock();
        }
    }

    // Transaction operations
    public boolean prepareDebit(String transactionId, String accountId, double amount) {
        accountLock.writeLock().lock();
        try {
            Double balance = accounts.get(accountId);
            if (balance == null || balance < amount) {
                return false;
            }

            TransactionState state = new TransactionState(transactionId, accountId, amount, "DEBIT");
            pendingTransactions.put(transactionId, state);
            System.out.println("Prepared DEBIT: txn=" + transactionId + ", account=" + accountId + ", amount=" + amount);
            return true;
        } finally {
            accountLock.writeLock().unlock();
        }
    }

    public boolean prepareCredit(String transactionId, String accountId, double amount) {
        accountLock.writeLock().lock();
        try {
            if (!accounts.containsKey(accountId)) {
                return false;
            }

            TransactionState state = new TransactionState(transactionId, accountId, amount, "CREDIT");
            pendingTransactions.put(transactionId, state);
            System.out.println("Prepared CREDIT: txn=" + transactionId + ", account=" + accountId + ", amount=" + amount);
            return true;
        } finally {
            accountLock.writeLock().unlock();
        }
    }

    public boolean commitTransaction(String transactionId) {
        accountLock.writeLock().lock();
        try {
            TransactionState state = pendingTransactions.remove(transactionId);
            if (state == null) {
                return false;
            }

            Double currentBalance = accounts.get(state.accountId);
            if (currentBalance == null) {
                return false;
            }

            if ("DEBIT".equals(state.operation)) {
                accounts.put(state.accountId, currentBalance - state.amount);
            } else if ("CREDIT".equals(state.operation)) {
                accounts.put(state.accountId, currentBalance + state.amount);
            }

            System.out.println("Committed: txn=" + transactionId + ", " + state.operation + " " + state.amount + " on " + state.accountId);
            return true;
        } finally {
            accountLock.writeLock().unlock();
        }
    }

    public boolean abortTransaction(String transactionId) {
        TransactionState state = pendingTransactions.remove(transactionId);
        if (state != null) {
            System.out.println("Aborted: txn=" + transactionId);
            return true;
        }
        return false;
    }

    static class TransactionState {
        String transactionId;
        String accountId;
        double amount;
        String operation;

        TransactionState(String transactionId, String accountId, double amount, String operation) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.amount = amount;
            this.operation = operation;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: PartitionServer <partition_id> <port> <partition_index>");
            System.out.println("Example: PartitionServer PARTITION_A 11001 0");
            System.exit(1);
        }

        String partitionId = args[0];
        int port = Integer.parseInt(args[1]);
        int partitionIndex = Integer.parseInt(args[2]);

        PartitionServer server = new PartitionServer(partitionId, "localhost", port, partitionIndex);
        server.startServer();
    }
}