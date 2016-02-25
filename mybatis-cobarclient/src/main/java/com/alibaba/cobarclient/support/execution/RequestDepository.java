package com.alibaba.cobarclient.support.execution;

import java.sql.Connection;

/**
 * temporary status depository for request processing.<br>
 *
 * @author fujohnwang
 * @since 1.0
 */
public class RequestDepository {
    private ConcurrentRequest originalRequest;
    private Connection        connectionToUse;
    private boolean           transactionAware;

    public ConcurrentRequest getOriginalRequest() {
        return originalRequest;
    }

    public void setOriginalRequest(ConcurrentRequest originalRequest) {
        this.originalRequest = originalRequest;
    }

    public Connection getConnectionToUse() {
        return connectionToUse;
    }

    public void setConnectionToUse(Connection connectionToUse) {
        this.connectionToUse = connectionToUse;
    }

    public boolean isTransactionAware() {
        return transactionAware;
    }

    public void setTransactionAware(boolean transactionAware) {
        this.transactionAware = transactionAware;
    }

}