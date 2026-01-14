package com.ewallet.partition;

import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.UUID;

public class TransferServiceImpl extends TransferServiceGrpc.TransferServiceImplBase {
    private final PartitionServer server;
    private static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";

    public TransferServiceImpl(PartitionServer server) {
        this.server = server;
    }

    @Override
    public void transfer(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {
        String fromAccount = request.getFromAccountId();
        String toAccount = request.getToAccountId();
        double amount = request.getAmount();
        String transactionId = request.getTransactionId().isEmpty() ?
                UUID.randomUUID().toString() : request.getTransactionId();

        System.out.println("Transfer request: " + fromAccount + " -> " + toAccount + ", amount=" + amount);

        TransferResponse response;

        if (server.isLeader()) {
            // Act as primary
            boolean fromInThisPartition = server.hasAccount(fromAccount);
            boolean toInThisPartition = server.hasAccount(toAccount);

            if (fromInThisPartition && toInThisPartition) {
                // Within-partition transfer
                response = handleWithinPartitionTransfer(fromAccount, toAccount, amount, transactionId);
            } else {
                // Cross-partition transfer - use 2PC
                response = handleCrossPartitionTransfer(fromAccount, toAccount, amount, transactionId);
            }
        } else {
            // Act as secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing transfer on secondary, on Primary's command");
                boolean fromInThisPartition = server.hasAccount(fromAccount);
                boolean toInThisPartition = server.hasAccount(toAccount);

                if (fromInThisPartition && toInThisPartition) {
                    response = handleWithinPartitionTransfer(fromAccount, toAccount, amount, transactionId);
                } else {
                    response = handleCrossPartitionTransfer(fromAccount, toAccount, amount, transactionId);
                }
            } else {
                // Forward to primary
                System.out.println("Not leader, forwarding to primary...");
                response = callPrimary(fromAccount, toAccount, amount, transactionId);
            }
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private TransferResponse handleWithinPartitionTransfer(String fromAccount, String toAccount,
                                                           double amount, String transactionId) {
        System.out.println("Within-partition transfer: " + transactionId);

        // Prepare phase
        boolean debitPrepared = server.prepareDebit(transactionId, fromAccount, amount);
        if (!debitPrepared) {
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Insufficient balance")
                    .setTransactionId(transactionId)
                    .build();
        }

        boolean creditPrepared = server.prepareCredit(transactionId + "_credit", toAccount, amount);
        if (!creditPrepared) {
            server.abortTransaction(transactionId);
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Target account not found")
                    .setTransactionId(transactionId)
                    .build();
        }

        // Commit phase
        boolean debitCommitted = server.commitTransaction(transactionId);
        boolean creditCommitted = server.commitTransaction(transactionId + "_credit");

        if (debitCommitted && creditCommitted) {
            // Replicate to secondaries if this is the leader
            if (server.isLeader()) {
                replicateTransferToSecondaries(fromAccount, toAccount, amount, transactionId);
            }

            return TransferResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Transfer completed successfully")
                    .setTransactionId(transactionId)
                    .build();
        } else {
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Transfer commit failed")
                    .setTransactionId(transactionId)
                    .build();
        }
    }

    private TransferResponse handleCrossPartitionTransfer(String fromAccount, String toAccount,
                                                          double amount, String transactionId) {
        System.out.println("Cross-partition transfer: " + transactionId);

        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator(server);
        return coordinator.executeTransfer(fromAccount, toAccount, amount, transactionId);
    }

    @Override
    public void prepare(PrepareRequest request, StreamObserver<PrepareResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        String accountId = request.getAccountId();
        double amount = request.getAmount();
        String operation = request.getOperation();

        System.out.println("Prepare request: txn=" + transactionId + ", op=" + operation);

        boolean canCommit;
        if ("DEBIT".equals(operation)) {
            canCommit = server.prepareDebit(transactionId, accountId, amount);
        } else {
            canCommit = server.prepareCredit(transactionId, accountId, amount);
        }

        // IMPORTANT: Replicate prepare to secondaries if this is the leader
        if (canCommit && server.isLeader()) {
            replicatePrepareToSecondaries(transactionId, accountId, amount, operation);
        }

        PrepareResponse response = PrepareResponse.newBuilder()
                .setCanCommit(canCommit)
                .setTransactionId(transactionId)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        System.out.println("Commit request: txn=" + transactionId);

        boolean success = server.commitTransaction(transactionId);

        // IMPORTANT: Replicate commit to secondaries if this is the leader
        if (success && server.isLeader()) {
            replicateCommitToSecondaries(transactionId);
        }

        CommitResponse response = CommitResponse.newBuilder()
                .setSuccess(success)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void abort(AbortRequest request, StreamObserver<AbortResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        System.out.println("Abort request: txn=" + transactionId);

        boolean success = server.abortTransaction(transactionId);

        // IMPORTANT: Replicate abort to secondaries if this is the leader
        if (success && server.isLeader()) {
            replicateAbortToSecondaries(transactionId);
        }

        AbortResponse response = AbortResponse.newBuilder()
                .setSuccess(success)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private TransferResponse callPrimary(String fromAccount, String toAccount, double amount, String transactionId) {
        System.out.println("Calling Primary server via name service");
        try {
            // Discover primary/leader via name service
            String leaderServiceName = server.getPartitionId();
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(leaderServiceName);

            String IPAddress = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Found leader at: " + IPAddress + ":" + port);
            return callServer(fromAccount, toAccount, amount, transactionId, false, IPAddress, port);

        } catch (Exception e) {
            System.err.println("Error discovering or calling primary: " + e.getMessage());

            // Fallback: try using lock data
            try {
                String[] currentLeaderData = server.getCurrentLeaderData();
                if (currentLeaderData != null) {
                    String IPAddress = currentLeaderData[0];
                    int port = Integer.parseInt(currentLeaderData[1]);
                    System.out.println("Using fallback leader from lock: " + IPAddress + ":" + port);
                    return callServer(fromAccount, toAccount, amount, transactionId, false, IPAddress, port);
                }
            } catch (Exception ex) {
                System.err.println("Fallback also failed: " + ex.getMessage());
            }

            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Leader not available")
                    .build();
        }
    }

    private TransferResponse callServer(String fromAccount, String toAccount, double amount,
                                        String transactionId, boolean isSentByPrimary,
                                        String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(IPAddress, port)
                    .usePlaintext()
                    .build();

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

            TransferRequest request = TransferRequest.newBuilder()
                    .setFromAccountId(fromAccount)
                    .setToAccountId(toAccount)
                    .setAmount(amount)
                    .setTransactionId(transactionId)
                    .setIsSentByPrimary(isSentByPrimary)
                    .build();

            TransferResponse response = stub.transfer(request);
            return response;
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private void replicateTransferToSecondaries(String fromAccount, String toAccount,
                                                double amount, String transactionId) {
        try {
            System.out.println("Replicating transfer to secondary servers");
            List<String[]> othersData = server.getOthersData();

            for (String[] data : othersData) {
                String IPAddress = data[0];
                int port = Integer.parseInt(data[1]);

                try {
                    callServer(fromAccount, toAccount, amount, transactionId, true, IPAddress, port);
                    System.out.println("Successfully replicated to " + IPAddress + ":" + port);
                } catch (Exception e) {
                    System.err.println("Failed to replicate to " + IPAddress + ":" + port);
                }
            }
        } catch (Exception e) {
            System.err.println("Error during replication: " + e.getMessage());
        }
    }

    // NEW: Replicate prepare operation to secondaries
    private void replicatePrepareToSecondaries(String transactionId, String accountId,
                                               double amount, String operation) {
        try {
            System.out.println("Replicating prepare to secondary servers");
            List<String[]> othersData = server.getOthersData();

            for (String[] data : othersData) {
                String IPAddress = data[0];
                int port = Integer.parseInt(data[1]);

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(IPAddress, port)
                            .usePlaintext()
                            .build();

                    TransferServiceGrpc.TransferServiceBlockingStub stub =
                            TransferServiceGrpc.newBlockingStub(channel);

                    PrepareRequest request = PrepareRequest.newBuilder()
                            .setTransactionId(transactionId)
                            .setAccountId(accountId)
                            .setAmount(amount)
                            .setOperation(operation)
                            .build();

                    PrepareResponse response = stub.prepare(request);

                    if (response.getCanCommit()) {
                        System.out.println("Successfully replicated prepare to " + IPAddress + ":" + port);
                    } else {
                        System.err.println("Secondary prepare failed: " + IPAddress + ":" + port);
                    }

                } catch (Exception e) {
                    System.err.println("Failed to replicate prepare to " + IPAddress + ":" + port + " - " + e.getMessage());
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during prepare replication: " + e.getMessage());
        }
    }

    // NEW: Replicate commit operation to secondaries
    private void replicateCommitToSecondaries(String transactionId) {
        try {
            System.out.println("Replicating commit to secondary servers");
            List<String[]> othersData = server.getOthersData();

            for (String[] data : othersData) {
                String IPAddress = data[0];
                int port = Integer.parseInt(data[1]);

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(IPAddress, port)
                            .usePlaintext()
                            .build();

                    TransferServiceGrpc.TransferServiceBlockingStub stub =
                            TransferServiceGrpc.newBlockingStub(channel);

                    CommitRequest request = CommitRequest.newBuilder()
                            .setTransactionId(transactionId)
                            .build();

                    CommitResponse response = stub.commit(request);

                    if (response.getSuccess()) {
                        System.out.println("Successfully replicated commit to " + IPAddress + ":" + port);
                    } else {
                        System.err.println("Secondary commit failed: " + IPAddress + ":" + port);
                    }

                } catch (Exception e) {
                    System.err.println("Failed to replicate commit to " + IPAddress + ":" + port + " - " + e.getMessage());
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during commit replication: " + e.getMessage());
        }
    }

    // NEW: Replicate abort operation to secondaries
    private void replicateAbortToSecondaries(String transactionId) {
        try {
            System.out.println("Replicating abort to secondary servers");
            List<String[]> othersData = server.getOthersData();

            for (String[] data : othersData) {
                String IPAddress = data[0];
                int port = Integer.parseInt(data[1]);

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(IPAddress, port)
                            .usePlaintext()
                            .build();

                    TransferServiceGrpc.TransferServiceBlockingStub stub =
                            TransferServiceGrpc.newBlockingStub(channel);

                    AbortRequest request = AbortRequest.newBuilder()
                            .setTransactionId(transactionId)
                            .build();

                    AbortResponse response = stub.abort(request);

                    if (response.getSuccess()) {
                        System.out.println("Successfully replicated abort to " + IPAddress + ":" + port);
                    } else {
                        System.err.println("Secondary abort failed: " + IPAddress + ":" + port);
                    }

                } catch (Exception e) {
                    System.err.println("Failed to replicate abort to " + IPAddress + ":" + port + " - " + e.getMessage());
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during abort replication: " + e.getMessage());
        }
    }
}