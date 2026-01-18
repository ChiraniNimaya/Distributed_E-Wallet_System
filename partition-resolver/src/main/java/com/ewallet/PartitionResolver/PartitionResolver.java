package com.ewallet.PartitionResolver;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class PartitionResolver {

    private PartitionResolver() {
    }

    public static final int NUM_ACCOUNT_PER_PARTITION = 100;

    public static String resolve(String accountId, int partitionCount) {
        validate(accountId, partitionCount);

        int partitionIndex = (Integer.parseInt(accountId) <= NUM_ACCOUNT_PER_PARTITION) ? 0 : 1;

        return "PARTITION_" + partitionIndex;
    }

    private static void validate(String accountId, int partitionCount) {
        if (accountId == null || accountId.isEmpty()) {
            throw new IllegalArgumentException("Account Id cannot be null or empty");
        }

        if (!accountId.matches("\\d+")) {
            throw new IllegalArgumentException("Account Id must be numeric");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException("PartitionCount must be > 0");
        }
    }
}
