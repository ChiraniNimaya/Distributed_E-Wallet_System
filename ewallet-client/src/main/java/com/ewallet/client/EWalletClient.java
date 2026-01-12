package com.ewallet.client;

import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

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

        String partitionId = determinePartition(accountId);
        System.out.println("Account will be created in partition: " + partitionId);

        ManagedChannel channel = null;
        try {
            String leaderServiceName = "partition_" + partitionId + "_leader";
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);

            NameServiceClient.ServiceDetails serviceDetails;
            try {
                serviceDetails = nsClient.findService(leaderServiceName);
                System.out.println("Found leader via name service: " +
                        serviceDetails.getIPAddress() + ":" + serviceDetails.getPort());
            } catch (Exception e) {
                // Leader not found, try generic partition service (any replica will forward)
                System.out.println("Leader not found, trying partition service...");
                String partitionServiceName = "partition_" + partitionId;
                serviceDetails = nsClient.findService(partitionServiceName);
            }

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            AccountServiceGrpc.AccountServiceBlockingStub stub =
                    AccountServiceGrpc.newBlockingStub(channel);

            CreateAccountRequest request = CreateAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .setInitialBalance(balance)
                    .setIsSentByPrimary(false)
                    .build();

            CreateAccountResponse response = stub.createAccount(request);

            if (response.getSuccess()) {
                System.out.println("✓ Account created successfully in partition: " + response.getPartitionId());
            } else {
                System.out.println("✗ Failed to create account: " + response.getMessage());
            }
        } catch (Exception e) {
            System.out.println("Error creating account: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                }
            }
        }
    }

    private void checkBalance() {
        System.out.print("Enter account ID: ");
        String accountId = scanner.nextLine().trim();

        String[] partitions = {"PARTITION_A", "PARTITION_B"};

        for (String partitionId : partitions) {
            ManagedChannel channel = null;
            try {
                // Try to discover partition service via name service
                String partitionServiceName = "partition_" + partitionId;
                NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
                NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(partitionServiceName);

                String host = serviceDetails.getIPAddress();
                int port = serviceDetails.getPort();

                channel = ManagedChannelBuilder
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
                    return;
                }
            } catch (Exception e) {
                // Try next partition
                System.out.println("Account not found in " + partitionId + ", trying next partition...");
            } finally {
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        channel.shutdownNow();
                    }
                }
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

        String fromPartitionId = determinePartition(fromAccount);

        ManagedChannel channel = null;
        try {
            // Discover partition service via name service
            String partitionServiceName = "partition_" + fromPartitionId;
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(partitionServiceName);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Connecting to partition service: " + host + ":" + port);

            channel = ManagedChannelBuilder
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
        } catch (Exception e) {
            System.out.println("Error during transfer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                }
            }
        }
    }

    private String determinePartition(String accountId) {
        String significantPart = accountId;
        if (accountId.contains("_")) {
            significantPart = accountId.substring(accountId.lastIndexOf("_") + 1);
        }

        char firstChar = significantPart.toUpperCase().charAt(0);
        if (firstChar >= 'A' && firstChar <= 'M') {
            return "PARTITION_A";
        } else {
            return "PARTITION_B";
        }
    }

    private List<Integer> getReplicaPorts(String partitionId) {
        List<Integer> ports = new ArrayList<>();
        if ("PARTITION_A".equals(partitionId)) {
            ports.add(11001);
            ports.add(11002);
            ports.add(11003);
        } else {
            ports.add(12001);
            ports.add(12002);
            ports.add(12003);
        }
        return ports;
    }
}