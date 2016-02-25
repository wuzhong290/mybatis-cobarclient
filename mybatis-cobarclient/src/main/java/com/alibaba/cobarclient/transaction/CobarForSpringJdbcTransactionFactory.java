package com.alibaba.cobarclient.transaction;

/**
 * Created by wuzhong on 2016/2/19.
 */

import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.Transaction;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;

public class CobarForSpringJdbcTransactionFactory extends SpringManagedTransactionFactory{

    @Override
    public Transaction newTransaction(DataSource dataSource, TransactionIsolationLevel level, boolean autoCommit) {
        return new CobarJdbcTransaction(dataSource, level, autoCommit);
    }

    @Override
    public Transaction newTransaction(Connection conn) {
        return new CobarJdbcTransaction(conn);
    }

    @Override
    public void setProperties(Properties props) {
        super.setProperties(props);
    }
}
