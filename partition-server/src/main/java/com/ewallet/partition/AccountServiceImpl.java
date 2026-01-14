package com.ewallet.partition;

import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class AccountServiceImpl extends AccountServiceGrpc.AccountServiceImplBase {
    private final PartitionServer server;
    private static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";

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

        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Creating account as Primary");
                boolean success = server.createAccount(accountId, initialBalance);

                if (success) {
                    // Replicate to secondaries
                    updateSecondaryServers(accountId, initialBalance);

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
            } catch (Exception e) {
                System.out.println("Error while creating account: " + e.getMessage());
                e.printStackTrace();
                response = CreateAccountResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Error: " + e.getMessage())
                        .build();
            }
        } else {
            // Act as secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Creating account on secondary, on Primary's command");
                boolean success = server.createAccount(accountId, initialBalance);

                response = CreateAccountResponse.newBuilder()
                        .setSuccess(success)
                        .setMessage(success ? "Account created on secondary" : "Failed to create account")
                        .setPartitionId(server.getPartitionId())
                        .build();
            } else {
                // Forward to primary
                System.out.println("Forwarding the request to primary");
                response = callPrimary(accountId, initialBalance);
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

    private CreateAccountResponse callPrimary(String accountId, double initialBalance) {
        System.out.println("Calling Primary server via name service");
        try {
            // Discover primary/leader via name service
            String leaderServiceName = server.getPartitionId();
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(leaderServiceName);

            String IPAddress = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Found leader at: " + IPAddress + ":" + port);
            return callServer(accountId, initialBalance, false, IPAddress, port);

        } catch (Exception e) {
            System.err.println("Error discovering or calling primary: " + e.getMessage());

            // Fallback: try using lock data
            try {
                String[] currentLeaderData = server.getCurrentLeaderData();
                if (currentLeaderData != null) {
                    String IPAddress = currentLeaderData[0];
                    int port = Integer.parseInt(currentLeaderData[1]);
                    System.out.println("Using fallback leader from lock: " + IPAddress + ":" + port);
                    return callServer(accountId, initialBalance, false, IPAddress, port);
                }
            } catch (Exception ex) {
                System.err.println("Fallback also failed: " + ex.getMessage());
            }

            return CreateAccountResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Leader not available")
                    .build();
        }
    }

    private CreateAccountResponse callServer(String accountId, double initialBalance,
                                             boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(IPAddress, port)
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
            return response;
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private void updateSecondaryServers(String accountId, double initialBalance) {
        try {
            System.out.println("Updating secondary servers");
            List<String[]> othersData = server.getOthersData();

            for (String[] data : othersData) {
                String IPAddress = data[0];
                int port = Integer.parseInt(data[1]);

                try {
                    callServer(accountId, initialBalance, true, IPAddress, port);
                    System.out.println("Successfully replicated to " + IPAddress + ":" + port);
                } catch (Exception e) {
                    System.err.println("Failed to replicate to " + IPAddress + ":" + port);
                }
            }
        } catch (Exception e) {
            System.err.println("Error during replication: " + e.getMessage());
        }
    }
}