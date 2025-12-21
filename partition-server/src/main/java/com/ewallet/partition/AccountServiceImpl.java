package com.ewallet.partition;

import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class AccountServiceImpl extends AccountServiceGrpc.AccountServiceImplBase {
    private final PartitionServer server;

    public AccountServiceImpl(PartitionServer server) {
        this.server = server;
    }

    @Override
    public void createAccount(CreateAccountRequest request,
                              StreamObserver<CreateAccountResponse> responseObserver) {
        String accountId = request.getAccountId();
        double initialBalance = request.getInitialBalance();

        System.out.println("Received createAccount request: " + accountId);

        CreateAccountResponse response;

        if (!server.isLeader()) {
            // Forward to leader
            System.out.println("Not leader, forwarding to leader...");
            response = forwardCreateAccountToLeader(request);
        } else {
            // Process as leader
            boolean success = server.createAccount(accountId, initialBalance);

            if (success) {
                // Replicate to backups
                replicateToBackups(accountId, initialBalance);

                response = CreateAccountResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Account created successfully")
                        .setPartitionId(server.getPartitionId())
                        .build();
            } else {
                response = CreateAccountResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Account already exists")
                        .setPartitionId(server.getPartitionId())
                        .build();
            }
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getBalance(GetBalanceRequest request,
                           StreamObserver<GetBalanceResponse> responseObserver) {
        String accountId = request.getAccountId();
        System.out.println("Received getBalance request: " + accountId);

        Double balance = server.getBalance(accountId);

        GetBalanceResponse response;
        if (balance != null) {
            response = GetBalanceResponse.newBuilder()
                    .setBalance(balance)
                    .setSuccess(true)
                    .setMessage("Balance retrieved successfully")
                    .build();
        } else {
            response = GetBalanceResponse.newBuilder()
                    .setBalance(0.0)
                    .setSuccess(false)
                    .setMessage("Account not found in this partition")
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private CreateAccountResponse forwardCreateAccountToLeader(CreateAccountRequest request) {
        try {
            String[] leaderData = server.getCurrentLeaderData();
            if (leaderData == null) {
                return CreateAccountResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Leader not available")
                        .build();
            }

            String leaderHost = leaderData[0];
            int leaderPort = Integer.parseInt(leaderData[1]);

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(leaderHost, leaderPort)
                    .usePlaintext()
                    .build();

            AccountServiceGrpc.AccountServiceBlockingStub stub =
                    AccountServiceGrpc.newBlockingStub(channel);

            CreateAccountResponse response = stub.createAccount(request);
            channel.shutdown();

            return response;
        } catch (Exception e) {
            System.err.println("Error forwarding to leader: " + e.getMessage());
            return CreateAccountResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error forwarding to leader: " + e.getMessage())
                    .build();
        }
    }

    private void replicateToBackups(String accountId, double balance) {
        try {
            List<String[]> backups = server.getOthersData();
            System.out.println("Replicating account creation to " + backups.size() + " backups");

            for (String[] backup : backups) {
                String backupHost = backup[0];
                int backupPort = Integer.parseInt(backup[1]);

                new Thread(() -> {
                    try {
                        ManagedChannel channel = ManagedChannelBuilder
                                .forAddress(backupHost, backupPort)
                                .usePlaintext()
                                .build();

                        AccountServiceGrpc.AccountServiceBlockingStub stub =
                                AccountServiceGrpc.newBlockingStub(channel);

                        // Internal replication call
                        CreateAccountRequest replicaRequest = CreateAccountRequest.newBuilder()
                                .setAccountId(accountId)
                                .setInitialBalance(balance)
                                .build();

                        stub.createAccount(replicaRequest);
                        channel.shutdown();

                        System.out.println("Replicated to backup: " + backupHost + ":" + backupPort);
                    } catch (Exception e) {
                        System.err.println("Failed to replicate to backup: " + e.getMessage());
                    }
                }).start();
            }
        } catch (Exception e) {
            System.err.println("Error during replication: " + e.getMessage());
        }
    }
}