package com.ewallet.partition;

import com.ewallet.PartitionResolver.PartitionResolver;
import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

import static com.ewallet.partition.PartitionServer.NUM_PARTITIONS;

public class TwoPhaseCommitCoordinator {
    private final PartitionServer server;

    public TwoPhaseCommitCoordinator(PartitionServer server) {
        this.server = server;
    }

    public TransferResponse executeTransfer(String fromAccount, String toAccount,
                                            double amount, String transactionId) {
        System.out.println("Starting 2PC for cross-partition transaction: " + transactionId);

        String fromPartitionId = PartitionResolver.resolve(fromAccount, NUM_PARTITIONS);
        String toPartitionId = PartitionResolver.resolve(toAccount, NUM_PARTITIONS);

        System.out.println("From partition: " + fromPartitionId + ", To partition: " + toPartitionId);

        // ===== PRE-VALIDATION FOR SOURCE PARTITION =====
        // Check source account before starting 2PC
        if (fromPartitionId.equals(server.getPartitionId())) {
            // Local partition - validate directly
            String validationError = validateSourceAccount(fromAccount, amount);
            if (validationError != null) {
                System.out.println("Source validation failed: " + validationError);
                return TransferResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage(validationError)
                        .setTransactionId(transactionId)
                        .build();
            }
        }
        // Note: Remote partition validation happens in prepare phase

        // ===== PHASE 1: PREPARE =====
        System.out.println("Phase 1: PREPARE");

        // Prepare DEBIT on source partition
        PrepareResult debitResult = prepareParticipantWithDetails(
                fromPartitionId, transactionId, fromAccount, amount, "DEBIT");

        if (!debitResult.success) {
            System.out.println("Prepare failed for debit on " + fromPartitionId + ": " + debitResult.errorMessage);
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(debitResult.errorMessage)
                    .setTransactionId(transactionId)
                    .build();
        }

        // Prepare CREDIT on destination partition
        PrepareResult creditResult = prepareParticipantWithDetails(
                toPartitionId, transactionId + "_credit", toAccount, amount, "CREDIT");

        if (!creditResult.success) {
            System.out.println("Prepare failed for credit on " + toPartitionId + ": " + creditResult.errorMessage);
            // Abort the debit preparation
            abortParticipant(fromPartitionId, transactionId);
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(creditResult.errorMessage)
                    .setTransactionId(transactionId)
                    .build();
        }

        // ===== PHASE 2: COMMIT =====
        System.out.println("Phase 2: COMMIT");

        // Commit DEBIT on source partition
        boolean fromCommitted = commitParticipant(fromPartitionId, transactionId);

        // Commit CREDIT on destination partition
        boolean toCommitted = commitParticipant(toPartitionId, transactionId + "_credit");

        if (fromCommitted && toCommitted) {
            System.out.println("2PC completed successfully - all partitions and replicas updated");
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

    /**
     * Validate source account before starting 2PC
     * @return null if valid, error message if invalid
     */
    private String validateSourceAccount(String fromAccount, double amount) {
        // Check if source account exists
        if (!server.hasAccount(fromAccount)) {
            return "Source account not found: " + fromAccount;
        }

        // Check balance
        Double fromBalance = server.getBalance(fromAccount);
        if (fromBalance == null) {
            return "Unable to retrieve balance for account: " + fromAccount;
        }

        if (fromBalance < amount) {
            return "Insufficient balance. Current balance: " + fromBalance + ", Required: " + amount;
        }

        return null; // Valid
    }

    private PrepareResult prepareParticipantWithDetails(String partitionId, String transactionId,
                                                        String accountId, double amount, String operation) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition - prepare locally
            boolean canCommit;
            String errorMessage = null;

            if ("DEBIT".equals(operation)) {
                canCommit = server.prepareDebit(transactionId, accountId, amount);
                if (!canCommit) {
                    // Determine specific error
                    if (!server.hasAccount(accountId)) {
                        errorMessage = "Source account not found: " + accountId;
                    } else {
                        Double balance = server.getBalance(accountId);
                        errorMessage = "Insufficient balance. Current balance: " + balance + ", Required: " + amount;
                    }
                }
            } else {
                canCommit = server.prepareCredit(transactionId, accountId, amount);
                if (!canCommit) {
                    errorMessage = "Destination account not found: " + accountId;
                }
            }

            // If this is the leader and prepare succeeded, replicate to local secondaries
            if (canCommit && server.isLeader()) {
                replicatePrepareToLocalSecondaries(transactionId, accountId, amount, operation);
            }

            return new PrepareResult(canCommit, errorMessage);
        } else {
            // Remote partition - contact the leader, which will handle its own replication
            return prepareRemoteParticipantWithDetails(partitionId, transactionId, accountId, amount, operation);
        }
    }

    private boolean commitParticipant(String partitionId, String transactionId) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition - commit locally
            boolean success = server.commitTransaction(transactionId);

            // If this is the leader and commit succeeded, replicate to local secondaries
            if (success && server.isLeader()) {
                replicateCommitToLocalSecondaries(transactionId);
            }
            return success;
        } else {
            // Remote partition - contact the leader, which will handle its own replication
            return commitRemoteParticipant(partitionId, transactionId);
        }
    }

    private boolean abortParticipant(String partitionId, String transactionId) {
        if (partitionId.equals(server.getPartitionId())) {
            // Local partition - abort locally
            boolean success = server.abortTransaction(transactionId);

            // If this is the leader and abort succeeded, replicate to local secondaries
            if (success && server.isLeader()) {
                replicateAbortToLocalSecondaries(transactionId);
            }
            return success;
        } else {
            // Remote partition - contact the leader, which will handle its own replication
            return abortRemoteParticipant(partitionId, transactionId);
        }
    }

    /* ---------------- LOCAL REPLICATION HELPERS ---------------- */

    private void replicatePrepareToLocalSecondaries(String transactionId, String accountId,
                                                    double amount, String operation) {
        try {
            System.out.println("Replicating prepare to local secondaries");
            List<String[]> othersData = server.getOthersData();

            for (String[] replica : othersData) {
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

                    PrepareResponse response = stub.prepare(request);

                    if (response.getCanCommit()) {
                        System.out.println("Replicated prepare to secondary: " + host + ":" + port);
                    } else {
                        System.err.println("Secondary prepare failed: " + host + ":" + port);
                    }

                } catch (Exception e) {
                    System.err.println("Error replicating prepare to " + host + ":" + port + " - " + e.getMessage());
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
            List<String[]> othersData = server.getOthersData();

            for (String[] replica : othersData) {
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

                    CommitResponse response = stub.commit(request);

                    if (response.getSuccess()) {
                        System.out.println("Replicated commit to secondary: " + host + ":" + port);
                    } else {
                        System.err.println("Secondary commit failed: " + host + ":" + port);
                    }

                } catch (Exception e) {
                    System.err.println("Error replicating commit to " + host + ":" + port + " - " + e.getMessage());
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
            List<String[]> othersData = server.getOthersData();

            for (String[] replica : othersData) {
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

                    AbortResponse response = stub.abort(request);

                    if (response.getSuccess()) {
                        System.out.println("Replicated abort to secondary: " + host + ":" + port);
                    } else {
                        System.err.println("Secondary abort failed: " + host + ":" + port);
                    }

                } catch (Exception e) {
                    System.err.println("Error replicating abort to " + host + ":" + port + " - " + e.getMessage());
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

    /* ---------------- REMOTE PARTICIPANT OPERATIONS ---------------- */

    private PrepareResult prepareRemoteParticipantWithDetails(String partitionId, String transactionId,
                                                              String accountId, double amount, String operation) {
        ManagedChannel channel = null;
        try {
            // Find leader via name service
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

            if (response.getCanCommit()) {
                return new PrepareResult(true, null);
            } else {
                // Determine specific error based on operation
                String errorMessage;
                if ("DEBIT".equals(operation)) {
                    errorMessage = "Source account not found or insufficient balance in partition " + partitionId;
                } else {
                    errorMessage = "Destination account not found: " + accountId;
                }
                return new PrepareResult(false, errorMessage);
            }

        } catch (Exception e) {
            System.err.println("Error preparing remote participant " + partitionId + ": " + e.getMessage());
            String errorMessage = "Failed to contact partition " + partitionId + ": " + e.getMessage();
            return new PrepareResult(false, errorMessage);
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

    private static class PrepareResult {
        final boolean success;
        final String errorMessage;

        PrepareResult(boolean success, String errorMessage) {
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }
}