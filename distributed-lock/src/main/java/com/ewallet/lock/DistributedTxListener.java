package com.ewallet.lock;

public interface DistributedTxListener {
    void onGlobalCommit();
    void onGlobalAbort();
}
