package com.ewallet.client;

import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

public class EWalletClient {
    private static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private Scanner scanner = new Scanner(System.in);

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

            switch (choice) {
                case "1":
                    checkBalance();
                    break;
                case "2":
                    transferMoney();
                    break;
                case "3":
                    System.out.println("Goodbye!");
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
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

            switch (choice) {
                case "1":
                    createAccount();
                    break;
                case "2":
                    checkBalance();
                    break;
                case "3":
                    System.out.println("Goodbye!");
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    private void createAccount() {
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

        try {
            // Connect to any replica of the partition (will forward to leader)
            String serviceName = "partition_" + partitionId + "_replica_" + getFirstReplicaPort(partitionId);
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(serviceName);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            AccountServiceGrpc.AccountServiceBlockingStub stub =
                    AccountServiceGrpc.newBlockingStub(channel);

            CreateAccountRequest request = CreateAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .setInitialBalance(balance)
                    .build();

            CreateAccountResponse response = stub.createAccount(request);

            if (response.getSuccess()) {
                System.out.println("✓ Account created successfully in partition: " + response.getPartitionId());
            } else {
                System.out.println("✗ Failed to create account: " + response.getMessage());
            }

            channel.shutdown();
        } catch (Exception e) {
            System.out.println("Error creating account: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void checkBalance() {
        System.out.print("Enter account ID: ");
        String accountId = scanner.nextLine().trim();

        // Try both partitions to find the account
        String[] partitions = {"PARTITION_A", "PARTITION_B"};

        for (String partitionId : partitions) {
            try {
                String serviceName = "partition_" + partitionId + "_replica_" + getFirstReplicaPort(partitionId);
                NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
                NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(serviceName);

                String host = serviceDetails.getIPAddress();
                int port = serviceDetails.getPort();

                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build();

                AccountServiceGrpc.AccountServiceBlockingStub stub =
                        AccountServiceGrpc.newBlockingStub(channel);

                GetBalanceRequest request = GetBalanceRequest.newBuilder()
                        .setAccountId(accountId)
                        .build();

                GetBalanceResponse response = stub.getBalance(request);

                if (response.getSuccess()) {
                    System.out.println("✓ Balance for account " + accountId + ": $" +
                            String.format("%.2f", response.getBalance()));
                    channel.shutdown();
                    return;
                }

                channel.shutdown();
            } catch (Exception e) {
                // Try next partition
            }
        }

        System.out.println("✗ Account not found in any partition");
    }

    private void transferMoney() {
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

        try {
            // Connect to the source account's partition
            String serviceName = "partition_" + fromPartitionId + "_replica_" + getFirstReplicaPort(fromPartitionId);
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(serviceName);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            TransferServiceGrpc.TransferServiceBlockingStub stub =
                    TransferServiceGrpc.newBlockingStub(channel);

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
                System.out.println("✓ Transfer completed successfully!");
                System.out.println("  Transaction ID: " + response.getTransactionId());
            } else {
                System.out.println("✗ Transfer failed: " + response.getMessage());
            }

            channel.shutdown();
        } catch (Exception e) {
            System.out.println("Error during transfer: " + e.getMessage());
            e.printStackTrace();
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