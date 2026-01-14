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
        System.out.println("Starting 2PC for cross-partition transaction: " + transactionId);

        String fromPartitionId = determinePartition(fromAccount);
        String toPartitionId = determinePartition(toAccount);

        System.out.println("From partition: " + fromPartitionId + ", To partition: " + toPartitionId);

        // ===== PHASE 1: PREPARE =====
        System.out.println("Phase 1: PREPARE");

        // Prepare DEBIT on source partition (leader will replicate to its secondaries)
        boolean fromPrepared = prepareParticipant(
                fromPartitionId, transactionId, fromAccount, amount, "DEBIT");

        if (!fromPrepared) {
            System.out.println("Prepare failed for debit on " + fromPartitionId);
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Insufficient balance or account not found")
                    .setTransactionId(transactionId)
                    .build();
        }

        // Prepare CREDIT on destination partition (leader will replicate to its secondaries)
        boolean toPrepared = prepareParticipant(
                toPartitionId, transactionId + "_credit", toAccount, amount, "CREDIT");

        if (!toPrepared) {
            System.out.println("Prepare failed for credit on " + toPartitionId);
            // Abort the debit preparation (leader will replicate abort to its secondaries)
            abortParticipant(fromPartitionId, transactionId);
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Target account not found")
                    .setTransactionId(transactionId)
                    .build();
        }

        // ===== PHASE 2: COMMIT =====
        System.out.println("Phase 2: COMMIT");

        // Commit DEBIT on source partition (leader will replicate to its secondaries)
        boolean fromCommitted = commitParticipant(fromPartitionId, transactionId);

        // Commit CREDIT on destination partition (leader will replicate to its secondaries)
        boolean toCommitted = commitParticipant(toPartitionId, transactionId + "_credit");

        if (fromCommitted && toCommitted) {
            System.out.println("2PC completed successfully - all replicas updated");
            return TransferResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Cross-partition transfer completed successfully")
                    .setTransactionId(transactionId)
                    .build();
        } else {
            System.out.println("2PC commit failed");
            // Attempt to abort both (leaders will replicate aborts to their secondaries)
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
        String significantPart = accountId;
        if (accountId.contains("_")) {
            significantPart = accountId.substring(accountId.lastIndexOf("_") + 1);
        }

        char firstChar = significantPart.toUpperCase().charAt(0);
        if (firstChar >= 'A' && firstChar <= 'M') {
            return "PARTITION_0";
        } else {
            return "PARTITION_1";
        }
    }

    private boolean prepareParticipant(String partitionId, String transactionId,
                                       String accountId, double amount, String operation) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition - prepare locally (will replicate if leader)
            if ("DEBIT".equals(operation)) {
                boolean canCommit = server.prepareDebit(transactionId, accountId, amount);
                // If this is the leader and prepare succeeded, replicate to secondaries
                // This is now handled in TransferServiceImpl.prepare() when called via gRPC
                // For direct local calls, we need to handle it here
                if (canCommit && server.isLeader()) {
                    replicatePrepareToLocalSecondaries(transactionId, accountId, amount, operation);
                }
                return canCommit;
            } else {
                boolean canCommit = server.prepareCredit(transactionId, accountId, amount);
                if (canCommit && server.isLeader()) {
                    replicatePrepareToLocalSecondaries(transactionId, accountId, amount, operation);
                }
                return canCommit;
            }
        } else {
            // Remote partition - ask remote leader to prepare (leader will handle replication)
            return prepareRemoteParticipant(partitionId, transactionId, accountId, amount, operation);
        }
    }

    private boolean commitParticipant(String partitionId, String transactionId) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition - commit locally (will replicate if leader)
            boolean success = server.commitTransaction(transactionId);
            if (success && server.isLeader()) {
                replicateCommitToLocalSecondaries(transactionId);
            }
            return success;
        } else {
            // Remote partition - ask remote leader to commit (leader will handle replication)
            return commitRemoteParticipant(partitionId, transactionId);
        }
    }

    private boolean abortParticipant(String partitionId, String transactionId) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition - abort locally (will replicate if leader)
            boolean success = server.abortTransaction(transactionId);
            if (success && server.isLeader()) {
                replicateAbortToLocalSecondaries(transactionId);
            }
            return success;
        } else {
            // Remote partition - ask remote leader to abort (leader will handle replication)
            return abortRemoteParticipant(partitionId, transactionId);
        }
    }

    // Helper methods for local replication
    private void replicatePrepareToLocalSecondaries(String transactionId, String accountId,
                                                    double amount, String operation) {
        try {
            System.out.println("Replicating prepare to local secondaries");
            for (String[] replica : server.getOthersData()) {
                String host = replica[0];
                int port = Integer.parseInt(replica[1]);

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
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

                    stub.prepare(request);
                    System.out.println("Replicated prepare to secondary: " + host + ":" + port);

                } catch (Exception e) {
                    System.err.println("Error replicating prepare to " + host + ":" + port);
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during local prepare replication: " + e.getMessage());
        }
    }

    private void replicateCommitToLocalSecondaries(String transactionId) {
        try {
            System.out.println("Replicating commit to local secondaries");
            for (String[] replica : server.getOthersData()) {
                String host = replica[0];
                int port = Integer.parseInt(replica[1]);

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(host, port)
                            .usePlaintext()
                            .build();

                    TransferServiceGrpc.TransferServiceBlockingStub stub =
                            TransferServiceGrpc.newBlockingStub(channel);

                    CommitRequest request = CommitRequest.newBuilder()
                            .setTransactionId(transactionId)
                            .build();

                    stub.commit(request);
                    System.out.println("Replicated commit to secondary: " + host + ":" + port);

                } catch (Exception e) {
                    System.err.println("Error replicating commit to " + host + ":" + port);
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during local commit replication: " + e.getMessage());
        }
    }

    private void replicateAbortToLocalSecondaries(String transactionId) {
        try {
            System.out.println("Replicating abort to local secondaries");
            for (String[] replica : server.getOthersData()) {
                String host = replica[0];
                int port = Integer.parseInt(replica[1]);

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(host, port)
                            .usePlaintext()
                            .build();

                    TransferServiceGrpc.TransferServiceBlockingStub stub =
                            TransferServiceGrpc.newBlockingStub(channel);

                    AbortRequest request = AbortRequest.newBuilder()
                            .setTransactionId(transactionId)
                            .build();

                    stub.abort(request);
                    System.out.println("Replicated abort to secondary: " + host + ":" + port);

                } catch (Exception e) {
                    System.err.println("Error replicating abort to " + host + ":" + port);
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during local abort replication: " + e.getMessage());
        }
    }

    // Remote participant operations (leader will handle its own replication)
    private boolean prepareRemoteParticipant(String partitionId, String transactionId,
                                             String accountId, double amount, String operation) {
        ManagedChannel channel = null;
        try {
            // Find leader via name service (registered as just partitionId)
            NameServiceClient nsClient = new NameServiceClient(PartitionServer.NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(partitionId);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Found partition " + partitionId + " leader: " + host + ":" + port);

            channel = ManagedChannelBuilder
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
            // Remote leader will automatically replicate to its secondaries
            return response.getCanCommit();

        } catch (Exception e) {
            System.err.println("Error preparing remote participant " + partitionId + ": " + e.getMessage());
            return false;
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private boolean commitRemoteParticipant(String partitionId, String transactionId) {
        ManagedChannel channel = null;
        try {
            // Find leader via name service
            NameServiceClient nsClient = new NameServiceClient(PartitionServer.NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(partitionId);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Committing on partition " + partitionId + " leader: " + host + ":" + port);

            channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

            CommitRequest request = CommitRequest.newBuilder()
                    .setTransactionId(transactionId)
                    .build();

            CommitResponse response = stub.commit(request);
            // Remote leader will automatically replicate to its secondaries
            return response.getSuccess();

        } catch (Exception e) {
            System.err.println("Error committing remote participant " + partitionId + ": " + e.getMessage());
            return false;
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private boolean abortRemoteParticipant(String partitionId, String transactionId) {
        ManagedChannel channel = null;
        try {
            // Find leader via name service
            NameServiceClient nsClient = new NameServiceClient(PartitionServer.NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(partitionId);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Aborting on partition " + partitionId + " leader: " + host + ":" + port);

            channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

            AbortRequest request = AbortRequest.newBuilder()
                    .setTransactionId(transactionId)
                    .build();

            AbortResponse response = stub.abort(request);
            // Remote leader will automatically replicate to its secondaries
            return response.getSuccess();

        } catch (Exception e) {
            System.err.println("Error aborting remote participant " + partitionId + ": " + e.getMessage());
            return false;
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }
}