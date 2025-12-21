package com.ewallet.partition;

import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class TwoPhaseCommitCoordinator {
    private final PartitionServer server;

    public TwoPhaseCommitCoordinator(PartitionServer server) {
        this.server = server;
    }

    public TransferResponse executeTransfer(String fromAccount, String toAccount,
                                            double amount, String transactionId) {
        System.out.println("Starting 2PC for transaction: " + transactionId);

        // Determine which partition has which account
        String fromPartitionId = determinePartition(fromAccount);
        String toPartitionId = determinePartition(toAccount);

        System.out.println("From partition: " + fromPartitionId + ", To partition: " + toPartitionId);

        // Phase 1: Prepare
        boolean fromPrepared = prepareParticipant(fromPartitionId, transactionId, fromAccount, amount, "DEBIT");
        if (!fromPrepared) {
            System.out.println("Prepare failed for debit");
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Insufficient balance or account not found")
                    .setTransactionId(transactionId)
                    .build();
        }

        boolean toPrepared = prepareParticipant(toPartitionId, transactionId + "_credit", toAccount, amount, "CREDIT");
        if (!toPrepared) {
            System.out.println("Prepare failed for credit");
            // Abort the debit preparation
            abortParticipant(fromPartitionId, transactionId);
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Target account not found")
                    .setTransactionId(transactionId)
                    .build();
        }

        // Phase 2: Commit
        boolean fromCommitted = commitParticipant(fromPartitionId, transactionId);
        boolean toCommitted = commitParticipant(toPartitionId, transactionId + "_credit");

        if (fromCommitted && toCommitted) {
            System.out.println("2PC completed successfully");
            return TransferResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Cross-partition transfer completed successfully")
                    .setTransactionId(transactionId)
                    .build();
        } else {
            System.out.println("2PC commit failed");
            // Attempt to abort both
            abortParticipant(fromPartitionId, transactionId);
            abortParticipant(toPartitionId, transactionId + "_credit");
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Transfer commit failed")
                    .setTransactionId(transactionId)
                    .build();
        }
    }

    private String determinePartition(String accountId) {
        // Simple hash-based partitioning
        // Accounts starting with A-M go to PARTITION_A, N-Z go to PARTITION_B
        char firstChar = accountId.toUpperCase().charAt(0);
        if (firstChar >= 'A' && firstChar <= 'M') {
            return "PARTITION_A";
        } else {
            return "PARTITION_B";
        }
    }

    private boolean prepareParticipant(String partitionId, String transactionId,
                                       String accountId, double amount, String operation) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition
            if ("DEBIT".equals(operation)) {
                return server.prepareDebit(transactionId, accountId, amount);
            } else {
                return server.prepareCredit(transactionId, accountId, amount);
            }
        } else {
            // Remote partition
            return prepareRemoteParticipant(partitionId, transactionId, accountId, amount, operation);
        }
    }

    private boolean commitParticipant(String partitionId, String transactionId) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition
            return server.commitTransaction(transactionId);
        } else {
            // Remote partition
            return commitRemoteParticipant(partitionId, transactionId);
        }
    }

    private boolean abortParticipant(String partitionId, String transactionId) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition
            return server.abortTransaction(transactionId);
        } else {
            // Remote partition
            return abortRemoteParticipant(partitionId, transactionId);
        }
    }

    private boolean prepareRemoteParticipant(String partitionId, String transactionId,
                                             String accountId, double amount, String operation) {
        try {
            // Find the leader of the target partition
            String serviceName = "partition_" + partitionId + "_leader";
            NameServiceClient nsClient = new NameServiceClient(PartitionServer.NAME_SERVICE_ADDRESS);

            NameServiceClient.ServiceDetails serviceDetails;
            try {
                serviceDetails = nsClient.findService(serviceName);
            } catch (Exception e) {
                // Leader not registered, try any replica
                serviceName = "partition_" + partitionId + "_replica_11001";
                serviceDetails = nsClient.findService(serviceName);
            }

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
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
            channel.shutdown();

            return response.getCanCommit();
        } catch (Exception e) {
            System.err.println("Error preparing remote participant: " + e.getMessage());
            return false;
        }
    }

    private boolean commitRemoteParticipant(String partitionId, String transactionId) {
        try {
            String serviceName = "partition_" + partitionId + "_leader";
            NameServiceClient nsClient = new NameServiceClient(PartitionServer.NAME_SERVICE_ADDRESS);

            NameServiceClient.ServiceDetails serviceDetails;
            try {
                serviceDetails = nsClient.findService(serviceName);
            } catch (Exception e) {
                serviceName = "partition_" + partitionId + "_replica_11001";
                serviceDetails = nsClient.findService(serviceName);
            }

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

            CommitRequest request = CommitRequest.newBuilder()
                    .setTransactionId(transactionId)
                    .build();

            CommitResponse response = stub.commit(request);
            channel.shutdown();

            return response.getSuccess();
        } catch (Exception e) {
            System.err.println("Error committing remote participant: " + e.getMessage());
            return false;
        }
    }

    private boolean abortRemoteParticipant(String partitionId, String transactionId) {
        try {
            String serviceName = "partition_" + partitionId + "_leader";
            NameServiceClient nsClient = new NameServiceClient(PartitionServer.NAME_SERVICE_ADDRESS);

            NameServiceClient.ServiceDetails serviceDetails;
            try {
                serviceDetails = nsClient.findService(serviceName);
            } catch (Exception e) {
                serviceName = "partition_" + partitionId + "_replica_11001";
                serviceDetails = nsClient.findService(serviceName);
            }

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

            AbortRequest request = AbortRequest.newBuilder()
                    .setTransactionId(transactionId)
                    .build();

            AbortResponse response = stub.abort(request);
            channel.shutdown();

            return response.getSuccess();
        } catch (Exception e) {
            System.err.println("Error aborting remote participant: " + e.getMessage());
            return false;
        }
    }
}