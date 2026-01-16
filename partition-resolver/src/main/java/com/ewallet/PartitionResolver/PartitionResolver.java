package com.ewallet.PartitionResolver;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class PartitionResolver {

    private PartitionResolver() {
    }

    public static String resolve(String accountId, int partitionCount) {
        validate(accountId, partitionCount);

        int hash = hashAccountId(accountId);
        int partitionIndex = Math.floorMod(hash, partitionCount);

        return "PARTITION_" + partitionIndex;
    }

    private static void validate(String accountId, int partitionCount) {
        if (accountId == null || accountId.isEmpty()) {
            throw new IllegalArgumentException("Account Id cannot be null or empty");
        }

        if (!accountId.matches("\\d+")) {
            throw new IllegalArgumentException("accountId must be numeric");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0");
        }
    }

    private static int hashAccountId(String accountId) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(accountId.getBytes(StandardCharsets.UTF_8));

            return ((digest[0] & 0xff) << 24)
                    | ((digest[1] & 0xff) << 16)
                    | ((digest[2] & 0xff) << 8)
                    | (digest[3] & 0xff);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
