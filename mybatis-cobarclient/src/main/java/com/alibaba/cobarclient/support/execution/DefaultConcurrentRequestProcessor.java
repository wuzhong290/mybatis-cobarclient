package com.alibaba.cobarclient.support.execution;

import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobarclient.support.SqlSessionCallback;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DefaultConcurrentRequestProcessor implements IConcurrentRequestProcessor {
    private final transient Logger logger = LoggerFactory.getLogger(DefaultConcurrentRequestProcessor.class);

    private SqlSessionFactory sqlSessionFactory;

    private ExecutorType executorType;

    public DefaultConcurrentRequestProcessor() {
    }

    public DefaultConcurrentRequestProcessor(SqlSessionFactory sqlSessionFactory, ExecutorType executorType) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.executorType = executorType;
    }

    public List<Object> process(List<ConcurrentRequest> requests) {
        ArrayList resultList = new ArrayList();
        Map<Connection, SqlSession> conToSession = new HashMap<Connection, SqlSession>();

        boolean isRealRequireClosedConnection = true;

        if(CollectionUtils.isEmpty(requests)) {
            return resultList;
        } else {
            List requestsDepo = this.fetchConnectionsAndDepositForLaterUse(requests);
            final CountDownLatch latch = new CountDownLatch(requestsDepo.size());
            ArrayList futures = new ArrayList();
            boolean var21 = false;

            Iterator i$;
            RequestDepository depo;
            try {
                var21 = true;
                i$ = requestsDepo.iterator();

                while(i$.hasNext()) {
                    depo = (RequestDepository)i$.next();
                    ConcurrentRequest springCon = depo.getOriginalRequest();
                    final SqlSessionCallback dataSource = springCon.getAction();
                    final Connection ex = depo.getConnectionToUse();
                    final SqlSession session = this.getSqlSessionFactory().openSession(getExecutorType(), ex);
                    conToSession.put(ex, session);
                    futures.add(springCon.getExecutor().submit(new Callable() {
                        public Object call() throws Exception {
                            Object var1;
                            try {
                                var1 = DefaultConcurrentRequestProcessor.this.executeWith(session, dataSource);
                            } finally {
                                latch.countDown();
                            }

                            return var1;
                        }
                    }));
                }

                try {
                    latch.await();
                    var21 = false;
                } catch (InterruptedException var24) {
                    throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency", var24);
                }
            } finally {
                if(var21) {
                    Iterator i$1 = requestsDepo.iterator();

                    while(i$1.hasNext()) {
                        RequestDepository depo1 = (RequestDepository)i$1.next();
                        Connection springCon1 = depo1.getConnectionToUse();
                        DataSource dataSource1 = depo1.getOriginalRequest().getDataSource();

                        try {
                            if(springCon1 != null) {
                                if(depo1.isTransactionAware()) {
                                    springCon1.close();
                                } else {
                                    DataSourceUtils.doReleaseConnection(springCon1, dataSource1);
                                }

                                isRealRequireClosedConnection = springCon1.isClosed();
                            }
                        } catch (Throwable var22) {
                            this.logger.info("Could not close JDBC Connection", var22);
                        }
                        try {
                            if (isRealRequireClosedConnection && springCon1 != null) {
                                SqlSession session = conToSession.get(springCon1);
                                if (session != null) {
                                    session.close();
                                }
                            }
                        } catch (Throwable ex) {
                            logger.info("不能关闭SqlSession,否则分布式事务无法commit", ex);
                        }
                    }

                }
            }

            i$ = requestsDepo.iterator();

            while(i$.hasNext()) {
                depo = (RequestDepository)i$.next();
                Connection springCon2 = depo.getConnectionToUse();
                DataSource dataSource2 = depo.getOriginalRequest().getDataSource();

                try {
                    if(springCon2 != null) {
                        if(depo.isTransactionAware()) {
                            springCon2.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(springCon2, dataSource2);
                        }
                        isRealRequireClosedConnection = springCon2.isClosed();
                    }
                } catch (Throwable var23) {
                    this.logger.info("Could not close JDBC Connection", var23);
                }
                try {
                    if (isRealRequireClosedConnection && springCon2 != null) {
                        SqlSession session = conToSession.get(springCon2);
                        if (session != null) {
                            session.close();
                        }
                    }
                } catch (Throwable ex) {
                    logger.info("不能关闭SqlSession,否则分布式事务无法commit", ex);
                }
            }

            this.fillResultListWithFutureResults(futures, resultList);
            return resultList;
        }
    }

    protected Object executeWith(SqlSession session, SqlSessionCallback action) {
        Object ex;

        try {
            ex = action.doInSqlSession(session);
        } catch (SQLException var9) {
            throw (new SQLErrorCodeSQLExceptionTranslator()).translate("SqlMapClient operation", (String)null, var9);
        }

        return ex;
    }

    private void fillResultListWithFutureResults(List<Future<Object>> futures, List<Object> resultList) {
        Iterator i$ = futures.iterator();

        while(i$.hasNext()) {
            Future future = (Future)i$.next();

            try {
                resultList.add(future.get());
            } catch (InterruptedException var6) {
                throw new ConcurrencyFailureException("interrupted when processing data access request in concurrency", var6);
            } catch (ExecutionException var7) {
                throw new ConcurrencyFailureException("something goes wrong in processing", var7);
            }
        }

    }

    private List<RequestDepository> fetchConnectionsAndDepositForLaterUse(List<ConcurrentRequest> requests) {
        ArrayList depos = new ArrayList();
        Iterator i$ = requests.iterator();

        while(i$.hasNext()) {
            ConcurrentRequest request = (ConcurrentRequest)i$.next();
            DataSource dataSource = request.getDataSource();
            Connection springCon = null;
            boolean transactionAware = dataSource instanceof TransactionAwareDataSourceProxy;

            try {
                springCon = transactionAware?dataSource.getConnection():DataSourceUtils.doGetConnection(dataSource);
            } catch (SQLException var9) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", var9);
            }

            RequestDepository depo = new RequestDepository();
            depo.setOriginalRequest(request);
            depo.setConnectionToUse(springCon);
            depo.setTransactionAware(transactionAware);
            depos.add(depo);
        }

        return depos;
    }

    public SqlSessionFactory getSqlSessionFactory() {
        return sqlSessionFactory;
    }

    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public ExecutorType getExecutorType() {
        return executorType;
    }

    public void setExecutorType(ExecutorType executorType) {
        this.executorType = executorType;
    }
}