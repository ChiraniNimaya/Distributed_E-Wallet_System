package com.ewallet.partition;

import com.ewallet.lock.*;
import com.ewallet.nameservice.NameServiceClient;
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
    private static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";

    // Transaction state holders
    private TransferData tempTransferData;
    private boolean transactionStatus = false;
    private String failureReason = "";

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

        if (server.isLeader()) {
            // Act as primary
            try {
                boolean fromInThisPartition = server.hasAccount(fromAccount);
                boolean toInThisPartition = server.hasAccount(toAccount);

                if (fromInThisPartition && toInThisPartition) {
                    // Within-partition transfer using distributed transaction
                    System.out.println("Within-partition transfer as Primary");

                    // PRE-VALIDATION: Check before starting distributed transaction
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
                    // Cross-partition transfer - use 2PC (traditional approach)
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
            }
        } else {
            // Act as secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing transfer on secondary, on Primary's command");
                DistributedTxParticipant txParticipant = new DistributedTxParticipant(this);
                startDistributedTxForTransfer(txParticipant, fromAccount, toAccount, amount, transactionId);

                // Validate the transfer
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
                    // Cross-partition on secondary - shouldn't happen normally
                    txParticipant.voteAbort();
                }
            } else {
                // Forward to primary
                System.out.println("Not leader, forwarding transfer to primary...");
                TransferResponse response = callPrimary(fromAccount, toAccount, amount, transactionId);
                if (response.getSuccess()) {
                    transactionStatus = true;
                }

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
        }

        TransferResponse response = TransferResponse.newBuilder()
                .setSuccess(transactionStatus)
                .setMessage(transactionStatus ? "Transfer completed" :
                        (failureReason.isEmpty() ? "Transfer failed" : failureReason))
                .setTransactionId(transactionId)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private String validateTransfer(String fromAccount, String toAccount, double amount) {
        // Check if source account exists
        if (!server.hasAccount(fromAccount)) {
            return "Source account not found: " + fromAccount;
        }

        // Check if destination account exists
        if (!server.hasAccount(toAccount)) {
            return "Destination account not found: " + toAccount;
        }

        // Check balance
        Double fromBalance = server.getBalance(fromAccount);
        if (fromBalance == null) {
            return "Unable to retrieve balance for account: " + fromAccount;
        }

        if (fromBalance < amount) {
            return "Insufficient balance. Current balance: " + fromBalance + ", Required: " + amount;
        }

        // Check for valid amount
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

    /* ---------------- TX CALLBACKS ---------------- */

    @Override
    public void onGlobalCommit() {
        executeTransfer();
    }

    @Override
    public void onGlobalAbort() {
        tempTransferData = null;
        transactionStatus = false;
        System.out.println("Transfer Transaction Aborted by the Coordinator");
    }

    /* ---------------- INTERNAL HELPERS ---------------- */

    private void executeTransfer() {
        if (tempTransferData != null) {
            String fromAccount = tempTransferData.fromAccount;
            String toAccount = tempTransferData.toAccount;
            double amount = tempTransferData.amount;
            String transactionId = tempTransferData.transactionId;

            // Execute the actual transfer
            boolean debitPrepared = server.prepareDebit(transactionId, fromAccount, amount);
            if (!debitPrepared) {
                System.out.println("Transfer failed: Insufficient balance");
                tempTransferData = null;
                transactionStatus = false;
                return;
            }

            boolean creditPrepared = server.prepareCredit(transactionId + "_credit", toAccount, amount);
            if (!creditPrepared) {
                server.abortTransaction(transactionId);
                System.out.println("Transfer failed: Target account not found");
                tempTransferData = null;
                transactionStatus = false;
                return;
            }

            boolean debitCommitted = server.commitTransaction(transactionId);
            boolean creditCommitted = server.commitTransaction(transactionId + "_credit");

            if (debitCommitted && creditCommitted) {
                System.out.println("Transfer from " + fromAccount + " to " + toAccount +
                        " amount " + amount + " committed");
                transactionStatus = true;
            } else {
                System.out.println("Transfer commit failed");
                transactionStatus = false;
            }

            tempTransferData = null;
        }
    }

    private void startDistributedTxForTransfer(DistributedTx tx, String fromAccount, String toAccount,
                                               double amount, String transactionId) {
        try {
            tx.start(transactionId, String.valueOf(UUID.randomUUID()));
            tempTransferData = new TransferData(fromAccount, toAccount, amount, transactionId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServersForTransfer(String fromAccount, String toAccount,
                                                   double amount, String transactionId)
            throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers for transfer");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(fromAccount, toAccount, amount, transactionId, true, IPAddress, port);
        }
    }

    private TransferResponse callPrimary(String fromAccount, String toAccount,
                                         double amount, String transactionId) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(fromAccount, toAccount, amount, transactionId, false, IPAddress, port);
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

    /* ---------------- DATA HOLDER CLASS ---------------- */

    private static class TransferData {
        String fromAccount;
        String toAccount;
        double amount;
        String transactionId;

        TransferData(String fromAccount, String toAccount, double amount, String transactionId) {
            this.fromAccount = fromAccount;
            this.toAccount = toAccount;
            this.amount = amount;
            this.transactionId = transactionId;
        }
    }
}