package com.ewallet.partition;

import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import com.ewallet.lock.*;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.io.IOException;
import java.util.UUID;

public class AccountServiceImpl extends AccountServiceGrpc.AccountServiceImplBase
        implements DistributedTxListener {

    private final PartitionServer server;
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";

    private AccountData tempDataHolder;
    private boolean transactionStatus = false;

    public AccountServiceImpl(PartitionServer server) {
        this.server = server;
    }

    @Override
    public void createAccount(CreateAccountRequest request,
                              StreamObserver<CreateAccountResponse> responseObserver) {

        String accountId = request.getAccountId();
        double initialBalance = request.getInitialBalance();

        if (server.isLeader()) {
            try {
                System.out.println("Creating account as Primary");
                startDistributedTx(accountId, initialBalance);
                updateSecondaryServers(accountId, initialBalance);
                System.out.println("Going to perform transaction");
                ((DistributedTxCoordinator) server.getTransaction()).perform();
            } catch (Exception e) {
                System.out.println("Error while creating account: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Creating account on secondary, on Primary's command");
                startDistributedTx(accountId, initialBalance);

                // Vote based on validation
                if (!server.hasAccount(accountId)) {
                    ((DistributedTxParticipant) server.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getTransaction()).voteAbort();
                }
            } else {
                CreateAccountResponse response = callPrimary(accountId, initialBalance);
                if (response.getSuccess()) {
                    transactionStatus = true;
                }
            }
        }

        CreateAccountResponse response = CreateAccountResponse
                .newBuilder()
                .setSuccess(transactionStatus)
                .setPartitionId(server.getPartitionId())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getBalance(GetBalanceRequest request,
                           StreamObserver<GetBalanceResponse> responseObserver) {

        String accountId = request.getAccountId();
        System.out.println("Received getBalance request for account: " + accountId);

        Double balance = server.getBalance(accountId);

        if (balance != null) {
            responseObserver.onNext(
                    GetBalanceResponse.newBuilder()
                            .setBalance(balance)
                            .setSuccess(true)
                            .setMessage("Balance retrieved from partition " + server.getPartitionId())
                            .build()
            );
            responseObserver.onCompleted();
            return;
        }

        if (server.isLeader()) {
            responseObserver.onNext(
                    GetBalanceResponse.newBuilder()
                            .setBalance(0.0)
                            .setSuccess(false)
                            .setMessage("Account not found in " + server.getPartitionId())
                            .build()
            );
            responseObserver.onCompleted();
            return;
        }

        System.out.println("Account not found locally, forwarding getBalance to leader");

        try {
            GetBalanceResponse leaderResponse = callPrimaryGetBalance(accountId);
            responseObserver.onNext(leaderResponse);
        } catch (Exception e) {
            responseObserver.onNext(
                    GetBalanceResponse.newBuilder()
                            .setBalance(0.0)
                            .setSuccess(false)
                            .setMessage("Failed to contact leader")
                            .build()
            );
        }

        responseObserver.onCompleted();
    }

    @Override
    public void onGlobalCommit() {
        updateAccount();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        transactionStatus = false;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    private void updateAccount() {
        if (tempDataHolder != null) {
            String accountId = tempDataHolder.accountId;
            double initialBalance = tempDataHolder.initialBalance;
            server.createAccount(accountId, initialBalance);
            System.out.println("Account " + accountId + " created with balance " + initialBalance + " committed");
            tempDataHolder = null;
            transactionStatus = true;
        }
    }

    private void startDistributedTx(String accountId, double initialBalance) {
        try {
            server.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new AccountData(accountId, initialBalance);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServers(String accountId, double initialBalance)
            throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(accountId, initialBalance, true, IPAddress, port);
        }
    }

    private CreateAccountResponse callPrimary(String accountId, double initialBalance) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, initialBalance, false, IPAddress, port);
    }

    private CreateAccountResponse callServer(String accountId, double initialBalance,
                                             boolean isSentByPrimary,
                                             String host, int port) {
        System.out.println("Call Server " + host + ":" + port);
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        AccountServiceGrpc.AccountServiceBlockingStub stub =
                AccountServiceGrpc.newBlockingStub(channel);

        CreateAccountRequest request = CreateAccountRequest.newBuilder()
                .setAccountId(accountId)
                .setInitialBalance(initialBalance)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        CreateAccountResponse response = stub.createAccount(request);
        channel.shutdown();
        return response;
    }

    private GetBalanceResponse callPrimaryGetBalance(String accountId) {
        try {
            String[] currentLeaderData = server.getCurrentLeaderData();
            if (currentLeaderData == null) {
                throw new RuntimeException("No leader data available");
            }

            String IPAddress = currentLeaderData[0];
            int port = Integer.parseInt(currentLeaderData[1]);

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(IPAddress, port)
                    .usePlaintext()
                    .build();

            AccountServiceGrpc.AccountServiceBlockingStub stub =
                    AccountServiceGrpc.newBlockingStub(channel);

            GetBalanceResponse response = stub.getBalance(
                    GetBalanceRequest.newBuilder()
                            .setAccountId(accountId)
                            .build()
            );

            channel.shutdown();
            return response;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class AccountData {
        String accountId;
        double initialBalance;

        AccountData(String accountId, double initialBalance) {
            this.accountId = accountId;
            this.initialBalance = initialBalance;
        }
    }
}