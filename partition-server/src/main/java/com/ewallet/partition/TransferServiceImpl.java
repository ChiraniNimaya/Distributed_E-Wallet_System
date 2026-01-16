package com.ewallet.partition;

import com.ewallet.lock.*;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class TransferServiceImpl extends TransferServiceGrpc.TransferServiceImplBase
        implements DistributedTxListener {

    private final PartitionServer server;

    private TransferData tempTransferData;
    private boolean transactionStatus = false;
    private String failureReason = "";

    private String pendingTransactionId;
    private String pendingOperation; // "DEBIT" or "CREDIT"

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

        // Reset state
        transactionStatus = false;
        failureReason = "";

        if (server.isLeader()) {
            try {
                boolean fromInThisPartition = server.hasAccount(fromAccount);
                boolean toInThisPartition = server.hasAccount(toAccount);

                if (fromInThisPartition && toInThisPartition) {
                    System.out.println("Within-partition transfer as Primary");

                    // Check before starting distributed transaction
                    String validationError = validateTransfer(fromAccount, toAccount, amount);
                    if (validationError != null) {
                        System.out.println("Transfer validation failed: " + validationError);
                        failureReason = validationError;
                        transactionStatus = false;

                        TransferResponse response = TransferResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage(validationError)
                                .setTransactionId(transactionId)
                                .build();

                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }

                    DistributedTxCoordinator txCoordinator = new DistributedTxCoordinator(this);
                    startDistributedTxForTransfer(txCoordinator, fromAccount, toAccount, amount, transactionId);
                    updateSecondaryServersForTransfer(fromAccount, toAccount, amount, transactionId);
                    System.out.println("Going to perform transfer transaction");
                    txCoordinator.perform();
                } else {
                    System.out.println("Cross-partition transfer using 2PC");
                    TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator(server);
                    TransferResponse crossPartitionResponse = coordinator.executeTransfer(
                            fromAccount, toAccount, amount, transactionId);

                    responseObserver.onNext(crossPartitionResponse);
                    responseObserver.onCompleted();
                    return;
                }
            } catch (Exception e) {
                System.out.println("Error while processing transfer: " + e.getMessage());
                e.printStackTrace();
                transactionStatus = false;
                failureReason = "Internal error: " + e.getMessage();
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing transfer on secondary, on Primary's command");

                DistributedTxParticipant txParticipant = new DistributedTxParticipant(this);
                startDistributedTxForTransfer(txParticipant, fromAccount, toAccount, amount, transactionId);

                boolean fromInThisPartition = server.hasAccount(fromAccount);
                boolean toInThisPartition = server.hasAccount(toAccount);

                if (fromInThisPartition && toInThisPartition) {
                    String validationError = validateTransfer(fromAccount, toAccount, amount);

                    if (validationError == null) {
                        System.out.println("Secondary validation passed - voting COMMIT");
                        txParticipant.voteCommit();
                    } else {
                        System.out.println("Secondary validation failed: " + validationError + " - voting ABORT");
                        txParticipant.voteAbort();
                    }
                } else {
                    System.out.println("Cross-partition request on secondary - voting ABORT");
                    txParticipant.voteAbort();
                }
            } else {
                System.out.println("Not leader, forwarding transfer to primary...");
                TransferResponse response = callPrimary(fromAccount, toAccount, amount, transactionId);

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
        }

        TransferResponse response = TransferResponse.newBuilder()
                .setSuccess(transactionStatus)
                .setMessage(transactionStatus ? "Transfer completed successfully" :
                        (failureReason.isEmpty() ? "Transfer failed" : failureReason))
                .setTransactionId(transactionId)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private String validateTransfer(String fromAccount, String toAccount, double amount) {
        if (!server.hasAccount(fromAccount)) {
            return "Source account not found: " + fromAccount;
        }

        if (!server.hasAccount(toAccount)) {
            return "Destination account not found: " + toAccount;
        }

        Double fromBalance = server.getBalance(fromAccount);
        if (fromBalance == null) {
            return "Unable to retrieve balance for account: " + fromAccount;
        }

        if (fromBalance < amount) {
            return "Insufficient balance. Current balance: " + fromBalance + ", Required: " + amount;
        }

        if (amount <= 0) {
            return "Invalid transfer amount: " + amount;
        }

        return null; // Valid
    }

    @Override
    public void prepare(PrepareRequest request, StreamObserver<PrepareResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        String accountId = request.getAccountId();
        double amount = request.getAmount();
        String operation = request.getOperation();
        boolean isSentByPrimary = request.getIsSentByPrimary();

        System.out.println("Prepare request: txn=" + transactionId + ", op=" + operation +
                ", account=" + accountId + ", amount=" + amount + ", isSentByPrimary=" + isSentByPrimary);

        boolean canCommit;

        if (server.isLeader() && !isSentByPrimary) {
            System.out.println("Leader received prepare request - will replicate to secondaries");

            // Store the operation details for execution on commit
            pendingTransactionId = transactionId;
            pendingOperation = operation;

            // Validate locally
            if ("DEBIT".equals(operation)) {
                canCommit = server.prepareDebit(transactionId, accountId, amount);
            } else {
                canCommit = server.prepareCredit(transactionId, accountId, amount);
            }

            if (canCommit) {
                // Start distributed transaction to replicate prepare to secondaries
                try {
                    DistributedTxCoordinator txCoordinator = new DistributedTxCoordinator(this);
                    txCoordinator.start(transactionId, String.valueOf(UUID.randomUUID()));

                    // Store the prepare information for secondaries
                    tempTransferData = new TransferData(accountId, accountId, amount, transactionId, operation);

                    // Replicate prepare to secondaries
                    replicatePrepareToSecondaries(transactionId, accountId, amount, operation);

                    System.out.println("Prepare replicated to secondaries");
                } catch (Exception e) {
                    System.err.println("Error replicating prepare: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } else if (!server.isLeader() && isSentByPrimary) {
            System.out.println("Secondary received prepare request from partition leader");

            if ("DEBIT".equals(operation)) {
                canCommit = server.prepareDebit(transactionId, accountId, amount);
            } else {
                canCommit = server.prepareCredit(transactionId, accountId, amount);
            }

            // Store for later commit
            pendingTransactionId = transactionId;
            pendingOperation = operation;
            tempTransferData = new TransferData(accountId, accountId, amount, transactionId, operation);
        } else {
            if ("DEBIT".equals(operation)) {
                canCommit = server.prepareDebit(transactionId, accountId, amount);
            } else {
                canCommit = server.prepareCredit(transactionId, accountId, amount);
            }
        }

        System.out.println("Prepare result: " + (canCommit ? "CAN_COMMIT" : "CANNOT_COMMIT"));

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
        boolean isSentByPrimary = request.getIsSentByPrimary();

        System.out.println("Commit request: txn=" + transactionId + ", isSentByPrimary=" + isSentByPrimary);

        boolean success;

        if (server.isLeader() && !isSentByPrimary) {
            // Commit locally
            success = server.commitTransaction(transactionId);

            if (success) {
                System.out.println("Leader committed locally, now replicating to secondaries");
                replicateCommitToSecondaries(transactionId);
            }
        } else if (!server.isLeader() && isSentByPrimary) {
            System.out.println("Secondary received commit from partition leader");
            success = server.commitTransaction(transactionId);

            pendingTransactionId = null;
            pendingOperation = null;
            tempTransferData = null;
        } else {
            success = server.commitTransaction(transactionId);
        }

        System.out.println("Commit result: " + (success ? "SUCCESS" : "FAILED"));

        CommitResponse response = CommitResponse.newBuilder()
                .setSuccess(success)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void abort(AbortRequest request, StreamObserver<AbortResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        boolean isSentByPrimary = request.getIsSentByPrimary();

        System.out.println("Abort request: txn=" + transactionId + ", isSentByPrimary=" + isSentByPrimary);

        boolean success;

        if (server.isLeader() && !isSentByPrimary) {
            success = server.abortTransaction(transactionId);

            if (success) {
                replicateAbortToSecondaries(transactionId);
            }
        } else if (!server.isLeader() && isSentByPrimary) {
            success = server.abortTransaction(transactionId);

            pendingTransactionId = null;
            pendingOperation = null;
            tempTransferData = null;
        } else {
            success = server.abortTransaction(transactionId);
        }

        System.out.println("Abort result: " + (success ? "SUCCESS" : "FAILED"));

        AbortResponse response = AbortResponse.newBuilder()
                .setSuccess(success)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void replicatePrepareToSecondaries(String transactionId, String accountId,
                                               double amount, String operation) {
        try {
            List<String[]> othersData = server.getOthersData();
            System.out.println("Replicating 2PC prepare to " + othersData.size() + " secondaries");

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
                            .setIsSentByPrimary(true)  // Mark as from primary
                            .build();

                    PrepareResponse response = stub.prepare(request);
                    System.out.println("Replicated prepare to secondary " + IPAddress + ":" + port +
                            " - result: " + response.getCanCommit());
                } catch (Exception e) {
                    System.err.println("Error replicating prepare to " + IPAddress + ":" + port +
                            " - " + e.getMessage());
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

    private void replicateCommitToSecondaries(String transactionId) {
        try {
            List<String[]> othersData = server.getOthersData();
            System.out.println("Replicating 2PC commit to " + othersData.size() + " secondaries");

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
                            .setIsSentByPrimary(true)  // Mark as from primary
                            .build();

                    CommitResponse response = stub.commit(request);
                    System.out.println("Replicated commit to secondary " + IPAddress + ":" + port +
                            " - result: " + response.getSuccess());
                } catch (Exception e) {
                    System.err.println("Error replicating commit to " + IPAddress + ":" + port +
                            " - " + e.getMessage());
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

    private void replicateAbortToSecondaries(String transactionId) {
        try {
            List<String[]> othersData = server.getOthersData();
            System.out.println("Replicating 2PC abort to " + othersData.size() + " secondaries");

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
                            .setIsSentByPrimary(true)  // Mark as from primary
                            .build();

                    AbortResponse response = stub.abort(request);
                    System.out.println("Replicated abort to secondary " + IPAddress + ":" + port +
                            " - result: " + response.getSuccess());
                } catch (Exception e) {
                    System.err.println("Error replicating abort to " + IPAddress + ":" + port +
                            " - " + e.getMessage());
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

    @Override
    public void onGlobalCommit() {
        System.out.println("Received GLOBAL_COMMIT callback");
        executeTransfer();
    }

    @Override
    public void onGlobalAbort() {
        System.out.println("Received GLOBAL_ABORT callback");
        tempTransferData = null;
        transactionStatus = false;
        failureReason = "Transaction aborted by coordinator";
        System.out.println("Transfer Transaction Aborted by the Coordinator");
    }

    private void executeTransfer() {
        if (tempTransferData != null) {
            String fromAccount = tempTransferData.fromAccount;
            String toAccount = tempTransferData.toAccount;
            double amount = tempTransferData.amount;
            String transactionId = tempTransferData.transactionId;

            System.out.println("Executing transfer: " + fromAccount + " -> " + toAccount +
                    ", amount=" + amount);

            boolean debitPrepared = server.prepareDebit(transactionId, fromAccount, amount);
            if (!debitPrepared) {
                System.out.println("Transfer failed: Insufficient balance");
                tempTransferData = null;
                transactionStatus = false;
                failureReason = "Insufficient balance";
                return;
            }

            boolean creditPrepared = server.prepareCredit(transactionId + "_credit", toAccount, amount);
            if (!creditPrepared) {
                server.abortTransaction(transactionId);
                System.out.println("Transfer failed: Target account not found");
                tempTransferData = null;
                transactionStatus = false;
                failureReason = "Target account not found";
                return;
            }

            boolean debitCommitted = server.commitTransaction(transactionId);
            boolean creditCommitted = server.commitTransaction(transactionId + "_credit");

            if (debitCommitted && creditCommitted) {
                System.out.println("Transfer from " + fromAccount + " to " + toAccount +
                        " amount " + amount + " committed");
                transactionStatus = true;
                failureReason = "";
            } else {
                System.out.println("Transfer commit failed");
                transactionStatus = false;
                failureReason = "Commit failed";
            }

            tempTransferData = null;
        } else {
            System.out.println("No pending transfer data to execute");
        }
    }

    private void startDistributedTxForTransfer(DistributedTx tx, String fromAccount,
                                               String toAccount, double amount, String transactionId) {
        try {
            tx.start(transactionId, String.valueOf(UUID.randomUUID()));
            tempTransferData = new TransferData(fromAccount, toAccount, amount, transactionId, null);
            System.out.println("Started distributed transaction and stored transfer data");
        } catch (IOException e) {
            System.err.println("Error starting distributed transaction: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void updateSecondaryServersForTransfer(String fromAccount, String toAccount,
                                                   double amount, String transactionId)
            throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers for transfer");
        List<String[]> othersData = server.getOthersData();

        if (othersData.isEmpty()) {
            System.out.println("No secondary servers to update");
            return;
        }

        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            System.out.println("Sending transfer to secondary: " + IPAddress + ":" + port);
            callServer(fromAccount, toAccount, amount, transactionId, true, IPAddress, port);
        }
    }

    private TransferResponse callPrimary(String fromAccount, String toAccount,
                                         double amount, String transactionId) {
        System.out.println("Calling Primary server");
        try {
            String[] currentLeaderData = server.getCurrentLeaderData();
            if (currentLeaderData == null) {
                System.err.println("No leader data available");
                return TransferResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Leader not available")
                        .setTransactionId(transactionId)
                        .build();
            }

            String IPAddress = currentLeaderData[0];
            int port = Integer.parseInt(currentLeaderData[1]);
            System.out.println("Primary is at: " + IPAddress + ":" + port);
            return callServer(fromAccount, toAccount, amount, transactionId, false, IPAddress, port);
        } catch (Exception e) {
            System.err.println("Error calling primary: " + e.getMessage());
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to contact primary")
                    .setTransactionId(transactionId)
                    .build();
        }
    }

    private TransferResponse callServer(String fromAccount, String toAccount, double amount,
                                        String transactionId, boolean isSentByPrimary,
                                        String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port +
                " (isSentByPrimary=" + isSentByPrimary + ")");
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
            System.out.println("Received response from " + IPAddress + ":" + port +
                    " - success=" + response.getSuccess());
            return response;
        } catch (Exception e) {
            System.err.println("Error calling server " + IPAddress + ":" + port + " - " + e.getMessage());
            return TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to contact server")
                    .setTransactionId(transactionId)
                    .build();
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private static class TransferData {
        String fromAccount;
        String toAccount;
        double amount;
        String transactionId;
        String operation;

        TransferData(String fromAccount, String toAccount, double amount, String transactionId, String operation) {
            this.fromAccount = fromAccount;
            this.toAccount = toAccount;
            this.amount = amount;
            this.transactionId = transactionId;
            this.operation = operation;
        }

        @Override
        public String toString() {
            return "TransferData{" +
                    "from='" + fromAccount + '\'' +
                    ", to='" + toAccount + '\'' +
                    ", amount=" + amount +
                    ", txId='" + transactionId + '\'' +
                    ", op='" + operation + '\'' +
                    '}';
        }
    }
}