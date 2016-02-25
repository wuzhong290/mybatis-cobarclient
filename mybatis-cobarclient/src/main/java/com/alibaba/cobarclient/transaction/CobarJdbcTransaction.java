package com.alibaba.cobarclient.transaction;

/**
 * Created by wuzhong on 2016/2/19.
 */

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.jdbc.JdbcTransaction;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class CobarJdbcTransaction extends JdbcTransaction {
    private static Log log = LogFactory.getLog(JdbcTransaction.class);

    public CobarJdbcTransaction(Connection connection) {
        super(connection);
    }

    public CobarJdbcTransaction(DataSource ds, TransactionIsolationLevel desiredLevel, boolean desiredAutoCommit) {
        super(DataSourceUtils.getConnection(ds));
    }

    @Override
    public void close() throws SQLException {
        try {
            if (getConnection() != null && !getConnection().isClosed()) {
                super.close();
            }
        } catch (SQLException e) {
            log.debug("Error closing the connection Cause: " + e);
        }
    }

    @Override
    public void commit() throws SQLException {
        try {
            if (getConnection() != null && !getConnection().isClosed()) {
                super.commit();
            }
        } catch (SQLException e) {
            log.debug("Error commiting the connection Cause: " + e);
        }
    }

    @Override
    public void rollback() throws SQLException {
        try {
            if (getConnection() != null && !getConnection().isClosed()) {
                super.rollback();
            }
        } catch (SQLException e) {
            log.debug("Error rollback the connection Cause: " + e);
        }
    }
}
