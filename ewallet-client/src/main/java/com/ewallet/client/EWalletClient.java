package com.ewallet.client;

import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.Scanner;

public class EWalletClient {
    private static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private Scanner scanner = new Scanner(System.in);

    // Connection management fields
    private ManagedChannel channel = null;
    private String host = null;
    private int port = -1;

    public static void main(String[] args) {
        String role = "client"; // default role

        if (args.length > 0) {
            role = args[0].toLowerCase();
        }

        EWalletClient client = new EWalletClient();

        if ("clerk".equals(role)) {
            client.runClerkMode();
        } else {
            client.runClientMode();
        }
    }

    public void runClientMode() {
        System.out.println("=== E-Wallet Client Mode ===");
        System.out.println("Available operations:");
        System.out.println("1. Check Balance");
        System.out.println("2. Transfer Money");
        System.out.println("3. Exit");

        while (true) {
            System.out.print("\nSelect operation (1-3): ");
            String choice = scanner.nextLine().trim();

            try {
                switch (choice) {
                    case "1":
                        checkBalance();
                        break;
                    case "2":
                        transferMoney();
                        break;
                    case "3":
                        System.out.println("Thank you for banking with us, Goodbye!");
                        return;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }

    public void runClerkMode() {
        System.out.println("=== E-Wallet Clerk Mode ===");
        System.out.println("Available operations:");
        System.out.println("1. Create Account");
        System.out.println("2. Check Balance");
        System.out.println("3. Exit");

        while (true) {
            System.out.print("\nSelect operation (1-3): ");
            String choice = scanner.nextLine().trim();

            try {
                switch (choice) {
                    case "1":
                        createAccount();
                        break;
                    case "2":
                        checkBalance();
                        break;
                    case "3":
                        System.out.println("Thank you for banking with us, Goodbye!");
                        return;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }

    private void createAccount() throws InterruptedException, IOException {
        System.out.print("Enter account ID: ");
        String accountId = scanner.nextLine().trim();

        System.out.print("Enter initial balance: ");
        double balance;
        try {
            balance = Double.parseDouble(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Invalid balance amount");
            return;
        }

        // Determine partition based on account ID
        String partitionId = determinePartition(accountId);
        System.out.println("Account will be created in partition: " + partitionId);

        // Fetch server details
        String serviceName = "partition_" + partitionId + "_replica_" + getFirstReplicaPort(partitionId);
        fetchServerDetails(serviceName);

        // Initialize connection
        initializeConnection();

        // Process create account request
        processCreateAccountRequest(accountId, balance, partitionId);

        // Close connection
        closeConnection();
    }

    private void checkBalance() throws InterruptedException, IOException {
        System.out.print("Enter account ID: ");
        String accountId = scanner.nextLine().trim();

        // Try both partitions to find the account
        String[] partitions = {"PARTITION_A", "PARTITION_B"};

        for (String partitionId : partitions) {
            try {
                String serviceName = "partition_" + partitionId + "_replica_" + getFirstReplicaPort(partitionId);

                // Fetch server details
                fetchServerDetails(serviceName);

                // Initialize connection
                initializeConnection();

                // Process balance check request
                boolean success = processBalanceCheckRequest(accountId);

                // Close connection
                closeConnection();

                if (success) {
                    return; // Account found, exit method
                }
            } catch (Exception e) {
                // Try next partition
                if (channel != null) {
                    closeConnection();
                }
            }
        }

        System.out.println("Account not found in any partition");
    }

    private void transferMoney() throws InterruptedException, IOException {
        System.out.print("Enter source account ID: ");
        String fromAccount = scanner.nextLine().trim();

        System.out.print("Enter destination account ID: ");
        String toAccount = scanner.nextLine().trim();

        System.out.print("Enter amount to transfer: ");
        double amount;
        try {
            amount = Double.parseDouble(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Invalid amount");
            return;
        }

        // Determine source partition
        String fromPartitionId = determinePartition(fromAccount);

        // Fetch server details
        String serviceName = "partition_" + fromPartitionId + "_replica_" + getFirstReplicaPort(fromPartitionId);
        fetchServerDetails(serviceName);

        // Initialize connection
        initializeConnection();

        // Process transfer request
        processTransferRequest(fromAccount, toAccount, amount);

        // Close connection
        closeConnection();
    }

    // Helper method to fetch server details from name service
    private void fetchServerDetails(String serviceName) throws IOException, InterruptedException {
        NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
        NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(serviceName);
        host = serviceDetails.getIPAddress();
        port = serviceDetails.getPort();
    }

    // Initialize connection to server
    private void initializeConnection() {
        System.out.println("Initializing connection to server at " + host + ":" + port);
        channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        channel.getState(true);
    }

    // Close connection
    private void closeConnection() {
        if (channel != null) {
            channel.shutdown();
        }
    }

    // Process create account request with connection state check
    private void processCreateAccountRequest(String accountId, double balance, String partitionId)
            throws InterruptedException, IOException {

        AccountServiceGrpc.AccountServiceBlockingStub stub =
                AccountServiceGrpc.newBlockingStub(channel);

        ConnectivityState state = channel.getState(true);

        while (state != ConnectivityState.READY) {
            System.out.println("Service unavailable, looking for a service provider..");
            String serviceName = "partition_" + partitionId + "_replica_" + getFirstReplicaPort(partitionId);
            fetchServerDetails(serviceName);
            initializeConnection();
            Thread.sleep(5000);
            state = channel.getState(true);
        }

        CreateAccountRequest request = CreateAccountRequest.newBuilder()
                .setAccountId(accountId)
                .setInitialBalance(balance)
                .build();

        CreateAccountResponse response = stub.createAccount(request);

        if (response.getSuccess()) {
            System.out.println("Account created successfully in partition: " + response.getPartitionId());
        } else {
            System.out.println("Failed to create account: " + response.getMessage());
        }
    }

    // Process balance check request with connection state check
    private boolean processBalanceCheckRequest(String accountId)
            throws InterruptedException, IOException {

        AccountServiceGrpc.AccountServiceBlockingStub stub =
                AccountServiceGrpc.newBlockingStub(channel);

        ConnectivityState state = channel.getState(true);

        while (state != ConnectivityState.READY) {
            System.out.println("Service unavailable, looking for a service provider..");
            fetchServerDetails("partition_" + determinePartition(accountId) + "_replica_" +
                    getFirstReplicaPort(determinePartition(accountId)));
            initializeConnection();
            Thread.sleep(5000);
            state = channel.getState(true);
        }

        GetBalanceRequest request = GetBalanceRequest.newBuilder()
                .setAccountId(accountId)
                .build();

        GetBalanceResponse response = stub.getBalance(request);

        if (response.getSuccess()) {
            System.out.println("Balance for account " + accountId + ": $" +
                    String.format("%.2f", response.getBalance()));
            return true;
        }

        return false;
    }

    // Process transfer request with connection state check
    private void processTransferRequest(String fromAccount, String toAccount, double amount)
            throws InterruptedException, IOException {

        TransferServiceGrpc.TransferServiceBlockingStub stub =
                TransferServiceGrpc.newBlockingStub(channel);

        ConnectivityState state = channel.getState(true);

        while (state != ConnectivityState.READY) {
            System.out.println("Service unavailable, looking for a service provider..");
            String fromPartitionId = determinePartition(fromAccount);
            String serviceName = "partition_" + fromPartitionId + "_replica_" +
                    getFirstReplicaPort(fromPartitionId);
            fetchServerDetails(serviceName);
            initializeConnection();
            Thread.sleep(5000);
            state = channel.getState(true);
        }

        TransferRequest request = TransferRequest.newBuilder()
                .setFromAccountId(fromAccount)
                .setToAccountId(toAccount)
                .setAmount(amount)
                .setTransactionId("")
                .setIsSentByPrimary(false)
                .build();

        System.out.println("Processing transfer...");
        TransferResponse response = stub.transfer(request);

        if (response.getSuccess()) {
            System.out.println("Transfer completed successfully!");
            System.out.println("Transaction ID: " + response.getTransactionId());
        } else {
            System.out.println("Transfer failed: " + response.getMessage());
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

    private String getFirstReplicaPort(String partitionId) {
        if ("PARTITION_A".equals(partitionId)) {
            return "11001";
        } else {
            return "12001";
        }
    }
}