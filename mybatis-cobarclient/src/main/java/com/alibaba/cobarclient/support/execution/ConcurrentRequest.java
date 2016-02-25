package com.alibaba.cobarclient.support.execution;

import com.alibaba.cobarclient.support.SqlSessionCallback;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;

/**
 * {@link #action} will be executed on {@link #dataSource} with
 * {@link #executor} asynchronously.<br>
 *
 * @author fujohnwang
 * @since 1.0
 */
public class ConcurrentRequest {
    private SqlSessionCallback action;
    private DataSource dataSource;
    private ExecutorService executor;
    private int dataSourceIndex;

    public SqlSessionCallback getAction() {
        return action;
    }

    public void setAction(SqlSessionCallback action) {
        this.action = action;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public int getDataSourceIndex() {
        return dataSourceIndex;
    }

    public void setDataSourceIndex(int dataSourceIndex) {
        this.dataSourceIndex = dataSourceIndex;
    }
}

