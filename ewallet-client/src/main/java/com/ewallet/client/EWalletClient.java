package com.ewallet.client;

import com.ewallet.PartitionResolver.PartitionResolver;
import com.ewallet.nameservice.NameServiceClient;
import com.ewallet.partition.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class EWalletClient {
    private static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private Scanner scanner = new Scanner(System.in);
    public static final int NUM_PARTITIONS = 2;

    public static void main(String[] args) {
        String role = "client";

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
        String accountId;
        while (true) {
            System.out.print("Enter account ID (numbers only): ");
            accountId = scanner.nextLine().trim();

            if (accountId.matches("\\d+")) {
                break;
            }

            System.out.println("Invalid account ID. Please enter numeric digits only.");
        }


        System.out.print("Enter initial balance: ");
        double balance;
        try {
            balance = Double.parseDouble(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Invalid balance amount");
            return;
        }

        String partitionId = PartitionResolver.resolve(accountId, NUM_PARTITIONS);
        System.out.println("Account will be created in partition: " + partitionId);

        ManagedChannel channel = null;
        try {
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails;
            try {
                serviceDetails = nsClient.findService(partitionId);
            } catch (Exception e) {
                System.out.println("Failed to find partition leader!");
                System.out.println("  Partition: " + partitionId);
                System.out.println("  Reason: " + e.getMessage());
                System.out.println("  Please ensure the partition servers are running.");
                return;
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
                System.out.println("Account created successfully in partition: " + response.getPartitionId());
            } else {
                System.out.println("Failed to create account: " + response.getMessage());
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

        String partitionId = PartitionResolver.resolve(accountId, NUM_PARTITIONS);
        System.out.println("Checking in partition: " + partitionId);

        ManagedChannel channel = null;
        try {
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(partitionId);

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
                System.out.println("Balance for account " + accountId + ": $" +
                        String.format("%.2f", response.getBalance()));
            } else {
                System.out.println("✗ " + response.getMessage());
            }

        } catch (Exception e) {
            System.out.println("Error checking balance: " + e.getMessage());
            System.out.println("  Account: " + accountId);
            System.out.println("  Partition: " + partitionId);
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

        String fromPartitionId = PartitionResolver.resolve(fromAccount, NUM_PARTITIONS);
        String toPartitionId = PartitionResolver.resolve(toAccount, NUM_PARTITIONS);

        System.out.println("Transfer route: " + fromPartitionId + " to " + toPartitionId);

        ManagedChannel channel = null;
        try {
            NameServiceClient nsClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
            NameServiceClient.ServiceDetails serviceDetails = nsClient.findService(fromPartitionId);

            String host = serviceDetails.getIPAddress();
            int port = serviceDetails.getPort();

            System.out.println("Connecting to source partition leader: " + host + ":" + port);

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
                System.out.println("Transfer completed successfully!");
                System.out.println("  Transaction ID: " + response.getTransactionId());
                System.out.println("  Amount: $" + String.format("%.2f", amount));
            } else {
                System.out.println("Transfer failed: " + response.getMessage());
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
}