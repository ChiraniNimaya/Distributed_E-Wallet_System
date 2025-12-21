package com.ewallet.partition;

import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.UUID;

public class TransferServiceImpl extends TransferServiceGrpc.TransferServiceImplBase {
    private final PartitionServer server;

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

        if (!server.isLeader() && !request.getIsSentByPrimary()) {
            // Forward to leader
            TransferResponse response = forwardToLeader(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        // Check if this is within-partition or cross-partition transfer
        boolean fromInThisPartition = server.hasAccount(fromAccount);
        boolean toInThisPartition = server.hasAccount(toAccount);

        TransferResponse response;

        if (fromInThisPartition && toInThisPartition) {
            // Within-partition transfer
            response = handleWithinPartitionTransfer(fromAccount, toAccount, amount, transactionId);
        } else {
            // Cross-partition transfer - use 2PC
            response = handleCrossPartitionTransfer(fromAccount, toAccount, amount, transactionId);
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
            // Replicate to backups
            replicateTransferToBackups(fromAccount, toAccount, amount, transactionId);

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

        AbortResponse response = AbortResponse.newBuilder()
                .setSuccess(success)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private TransferResponse forwardToLeader(TransferRequest request) {
        try {
            String[] leaderData = server.getCurrentLeaderData();
            if (leaderData == null) {
                return TransferResponse.newBuilder()
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

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

            TransferRequest forwardRequest = TransferRequest.newBuilder()
                    .setFromAccountId(request.getFromAccountId())
                    .setToAccountId(request.getToAccountId())
                    .setAmount(request.getAmount())
                    .setTransactionId(request.getTransactionId())
                    .setIsSentByPrimary(false)
                    .build();

            TransferResponse response = stub.transfer(forwardRequest);
            channel.shutdown();

            return response;
        } catch (Exception e) {
            System.err.println("Error forwarding to leader: " + e.getMessage());
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error forwarding to leader: " + e.getMessage())
                    .build();
        }
    }

    private void replicateTransferToBackups(String fromAccount, String toAccount,
                                            double amount, String transactionId) {
        try {
            List<String[]> backups = server.getOthersData();
            System.out.println("Replicating transfer to " + backups.size() + " backups");

            for (String[] backup : backups) {
                String backupHost = backup[0];
                int backupPort = Integer.parseInt(backup[1]);

                new Thread(() -> {
                    try {
                        ManagedChannel channel = ManagedChannelBuilder
                                .forAddress(backupHost, backupPort)
                                .usePlaintext()
                                .build();

                        TransferServiceGrpc.TransferServiceBlockingStub stub =
                                TransferServiceGrpc.newBlockingStub(channel);

                        TransferRequest replicaRequest = TransferRequest.newBuilder()
                                .setFromAccountId(fromAccount)
                                .setToAccountId(toAccount)
                                .setAmount(amount)
                                .setTransactionId(transactionId)
                                .setIsSentByPrimary(true)
                                .build();

                        stub.transfer(replicaRequest);
                        channel.shutdown();

                        System.out.println("Replicated transfer to backup: " + backupHost + ":" + backupPort);
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