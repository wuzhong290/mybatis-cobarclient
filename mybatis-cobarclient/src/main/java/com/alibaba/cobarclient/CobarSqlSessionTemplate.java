package com.alibaba.cobarclient;

import com.alibaba.cobar.client.audit.ISqlAuditor;
import com.alibaba.cobar.client.datasources.CobarDataSourceDescriptor;
import com.alibaba.cobar.client.datasources.ICobarDataSourceService;
import com.alibaba.cobar.client.exception.UncategorizedCobarClientException;
import com.alibaba.cobar.client.merger.IMerger;
import com.alibaba.cobar.client.router.ICobarRouter;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;
import com.alibaba.cobar.client.support.utils.Predicate;
import com.alibaba.cobar.client.support.vo.BatchInsertTask;
import com.alibaba.cobar.client.support.vo.CobarMRBase;
import com.alibaba.cobarclient.support.SqlSessionCallback;
import com.alibaba.cobarclient.support.execution.ConcurrentRequest;
import com.alibaba.cobarclient.support.execution.DefaultConcurrentRequestProcessor;
import com.alibaba.cobarclient.support.execution.IConcurrentRequestProcessor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.*;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by wuzhong on 2016/2/24.
 */
public class CobarSqlSessionTemplate extends SqlSessionTemplate implements InitializingBean, DisposableBean {

    private transient Logger logger = LoggerFactory.getLogger(CobarSqlSessionTemplate.class);

    private static final String DEFAULT_DATASOURCE_IDENTITY = "_CobarSqlMapClientTemplate_default_data_source_name";
    private String defaultDataSourceName = "_CobarSqlMapClientTemplate_default_data_source_name";
    private DataSource defaultDataSource;
    private List<ExecutorService> internalExecutorServiceRegistry = new ArrayList();
    private ICobarDataSourceService cobarDataSourceService;
    private ICobarRouter<IBatisRoutingFact> router;
    private ISqlAuditor sqlAuditor;
    private ExecutorService sqlAuditorExecutor;
    private Map<String, ExecutorService> dataSourceSpecificExecutors = new HashMap();
    private IConcurrentRequestProcessor concurrentRequestProcessor;
    private int defaultQueryTimeout = 100;
    private boolean profileLongTimeRunningSql = false;
    private long longTimeRunningSqlIntervalThreshold;
    private Map<String, IMerger<Object, Object>> mergers = new HashMap();

    public CobarSqlSessionTemplate(SqlSessionFactory sqlSessionFactory) {
        super(sqlSessionFactory);
    }

    public CobarSqlSessionTemplate(SqlSessionFactory sqlSessionFactory, ExecutorType executorType,
                                   PersistenceExceptionTranslator exceptionTranslator) {
        super(sqlSessionFactory, executorType, exceptionTranslator);
    }

    public CobarSqlSessionTemplate(SqlSessionFactory sqlSessionFactory, ExecutorType executorType) {
        super(sqlSessionFactory, executorType);
    }

    @Override
    public void destroy() throws Exception {
        logger.info("destroy instance");
        if(CollectionUtils.isNotEmpty(this.internalExecutorServiceRegistry)) {
            this.logger.info("shutdown executors of CobarSqlMapClientTemplate...");
            Iterator i$ = this.internalExecutorServiceRegistry.iterator();

            while(i$.hasNext()) {
                ExecutorService executor = (ExecutorService)i$.next();
                if(executor != null) {
                    try {
                        executor.shutdown();
                        executor.awaitTermination(5L, TimeUnit.MINUTES);
                        executor = null;
                    } catch (InterruptedException var4) {
                        this.logger.warn("interrupted when shuting down the query executor:\n{}", var4);
                    }
                }
            }

            this.getDataSourceSpecificExecutors().clear();
            this.logger.info("all of the executor services in CobarSqlMapClientTemplate are disposed.");
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if(this.isProfileLongTimeRunningSql() && this.longTimeRunningSqlIntervalThreshold <= 0L) {
            throw new IllegalArgumentException("\'longTimeRunningSqlIntervalThreshold\' should have a positive value if \'profileLongTimeRunningSql\' is set to true");
        } else {
            this.setupDefaultExecutorServicesIfNecessary();
            this.setUpDefaultSqlAuditorExecutorIfNecessary();
            if(this.getConcurrentRequestProcessor() == null) {
                this.setConcurrentRequestProcessor(new DefaultConcurrentRequestProcessor(this.getSqlSessionFactory(),getExecutorType()));
            }

        }
    }

    private void setupDefaultExecutorServicesIfNecessary() {
        if(this.isPartitioningBehaviorEnabled()) {
            if(MapUtils.isEmpty(this.getDataSourceSpecificExecutors())) {
                Set dataSourceDescriptors = this.getCobarDataSourceService().getDataSourceDescriptors();
                Iterator i$ = dataSourceDescriptors.iterator();

                while(i$.hasNext()) {
                    CobarDataSourceDescriptor descriptor = (CobarDataSourceDescriptor)i$.next();
                    ExecutorService executor = this.createExecutorForSpecificDataSource(descriptor);
                    this.getDataSourceSpecificExecutors().put(descriptor.getIdentity(), executor);
                }
            }

            this.addDefaultSingleThreadExecutorIfNecessary();
        }

    }

    protected boolean isPartitioningBehaviorEnabled() {
        return this.router != null && this.getCobarDataSourceService() != null;
    }

    private ExecutorService createExecutorForSpecificDataSource(CobarDataSourceDescriptor descriptor) {
        String identity = descriptor.getIdentity();
        final ExecutorService executor = this.createCustomExecutorService(descriptor.getPoolSize(), "createExecutorForSpecificDataSource-" + identity + " data source");
        this.internalExecutorServiceRegistry.add(executor);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(executor != null) {
                    try {
                        executor.shutdown();
                        executor.awaitTermination(5L, TimeUnit.MINUTES);
                    } catch (InterruptedException var2) {
                        CobarSqlSessionTemplate.this.logger.warn("interrupted when shuting down the query executor:\n{}", var2);
                    }

                }
            }
        });
        return executor;
    }

    private ExecutorService createCustomExecutorService(int poolSize, final String method) {
        int coreSize = Runtime.getRuntime().availableProcessors();
        if(poolSize < coreSize) {
            coreSize = poolSize;
        }

        ThreadFactory tf = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "thread created at CobarSqlMapClientTemplate method [" + method + "]");
                t.setDaemon(true);
                return t;
            }
        };
        LinkedBlockingQueue queueToUse = new LinkedBlockingQueue(coreSize);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(coreSize, poolSize, 60L, TimeUnit.SECONDS, queueToUse, tf, new ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }

    private void addDefaultSingleThreadExecutorIfNecessary() {
        String identity = this.getDefaultDataSourceName();
        CobarDataSourceDescriptor descriptor = new CobarDataSourceDescriptor();
        descriptor.setIdentity(identity);
        descriptor.setPoolSize(Runtime.getRuntime().availableProcessors() * 5);
        this.getDataSourceSpecificExecutors().put(identity, this.createExecutorForSpecificDataSource(descriptor));
    }

    private void setUpDefaultSqlAuditorExecutorIfNecessary() {
        if(this.sqlAuditor != null && this.sqlAuditorExecutor == null) {
            this.sqlAuditorExecutor = this.createCustomExecutorService(1, "setUpDefaultSqlAuditorExecutorIfNecessary");
            this.internalExecutorServiceRegistry.add(this.sqlAuditorExecutor);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    if(CobarSqlSessionTemplate.this.sqlAuditorExecutor != null) {
                        try {
                            CobarSqlSessionTemplate.this.sqlAuditorExecutor.shutdown();
                            CobarSqlSessionTemplate.this.sqlAuditorExecutor.awaitTermination(5L, TimeUnit.MINUTES);
                        } catch (InterruptedException var2) {
                            CobarSqlSessionTemplate.this.logger.warn("interrupted when shuting down the query executor:\n{}", var2);
                        }

                    }
                }
            });
        }

    }

    public String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public void setDefaultDataSourceName(String defaultDataSourceName) {
        this.defaultDataSourceName = defaultDataSourceName;
    }

    public ISqlAuditor getSqlAuditor() {
        return sqlAuditor;
    }

    public void setSqlAuditor(ISqlAuditor sqlAuditor) {
        this.sqlAuditor = sqlAuditor;
    }

    public ICobarDataSourceService getCobarDataSourceService() {
        return cobarDataSourceService;
    }

    public void setCobarDataSourceService(ICobarDataSourceService cobarDataSourceService) {
        this.cobarDataSourceService = cobarDataSourceService;
    }

    public ICobarRouter<IBatisRoutingFact> getRouter() {
        return router;
    }

    public void setRouter(ICobarRouter<IBatisRoutingFact> router) {
        this.router = router;
    }

    public IConcurrentRequestProcessor getConcurrentRequestProcessor() {
        return concurrentRequestProcessor;
    }

    public void setConcurrentRequestProcessor(IConcurrentRequestProcessor concurrentRequestProcessor) {
        this.concurrentRequestProcessor = concurrentRequestProcessor;
    }

    public int getDefaultQueryTimeout() {
        return defaultQueryTimeout;
    }

    public void setDefaultQueryTimeout(int defaultQueryTimeout) {
        this.defaultQueryTimeout = defaultQueryTimeout;
    }

    public boolean isProfileLongTimeRunningSql() {
        return profileLongTimeRunningSql;
    }

    public void setProfileLongTimeRunningSql(boolean profileLongTimeRunningSql) {
        this.profileLongTimeRunningSql = profileLongTimeRunningSql;
    }

    public long getLongTimeRunningSqlIntervalThreshold() {
        return longTimeRunningSqlIntervalThreshold;
    }

    public void setLongTimeRunningSqlIntervalThreshold(long longTimeRunningSqlIntervalThreshold) {
        this.longTimeRunningSqlIntervalThreshold = longTimeRunningSqlIntervalThreshold;
    }

    public Map<String, IMerger<Object, Object>> getMergers() {
        return mergers;
    }

    public void setMergers(Map<String, IMerger<Object, Object>> mergers) {
        this.mergers = mergers;
    }

    public Map<String, ExecutorService> getDataSourceSpecificExecutors() {
        return dataSourceSpecificExecutors;
    }

    public void setDataSourceSpecificExecutors(Map<String, ExecutorService> dataSourceSpecificExecutors) {
        this.dataSourceSpecificExecutors = dataSourceSpecificExecutors;
    }

    public ExecutorService getSqlAuditorExecutor() {
        return sqlAuditorExecutor;
    }

    public void setSqlAuditorExecutor(ExecutorService sqlAuditorExecutor) {
        this.sqlAuditorExecutor = sqlAuditorExecutor;
    }

    public DataSource getDefaultDataSource() {
        return defaultDataSource;
    }

    public void setDefaultDataSource(DataSource defaultDataSource) {
        this.defaultDataSource = defaultDataSource;
    }

    //上面代码为Bean的初始化代码，下面代码为SqlSessionTemplate的二次封装，增加cobarClient的功能

    @Override
    public <T> T selectOne(String statement) {
        return this.selectOne(statement, null);
    }

    @Override
    public <T> T selectOne(final String statement, final Object parameter) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var17 = false;

        T dsMap1;
        long interval3;
        T var10;
        long interval1;
        label171: {
            label172: {
                try {
                    var17 = true;
                    if(this.isPartitioningBehaviorEnabled()) {
                        SortedMap dsMap = this.lookupDataSourcesByRouter(statement, parameter);
                        if(!MapUtils.isEmpty(dsMap)) {
                            SqlSessionCallback interval = new SqlSessionCallback() {
                                public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                    return sqlSession.selectOne(statement,parameter);
                                }
                            };

                            List resultList = this.executeInConcurrency(interval, dsMap);
                            Collection<T> filteredResultList = CollectionUtils.select(resultList, new Predicate() {
                                public boolean evaluate(Object item) {
                                    return item != null;
                                }
                            });
                            if(filteredResultList.size() > 1) {
                                throw new IncorrectResultSizeDataAccessException(1);
                            }

                            if(CollectionUtils.isEmpty(filteredResultList)) {
                                var10 = null;
                                var17 = false;
                                break label171;
                            }

                            var10 = filteredResultList.iterator().next();
                            var17 = false;
                            break label172;
                        }
                    }

                    dsMap1 = super.selectOne(statement, parameter);
                    var17 = false;
                } finally {
                    if(var17) {
                        if(this.isProfileLongTimeRunningSql()) {
                            long interval2 = System.currentTimeMillis() - startTimestamp;
                            if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                            }
                        }

                    }
                }

                if(this.isProfileLongTimeRunningSql()) {
                    interval3 = System.currentTimeMillis() - startTimestamp;
                    if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                        this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                    }
                }

                return dsMap1;
            }

            if(this.isProfileLongTimeRunningSql()) {
                interval1 = System.currentTimeMillis() - startTimestamp;
                if(interval1 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval1)});
                }
            }

            return var10;
        }

        if(this.isProfileLongTimeRunningSql()) {
            interval1 = System.currentTimeMillis() - startTimestamp;
            if(interval1 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval1)});
            }
        }

        return var10;
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
        return this.selectMap(statement, null, mapKey, RowBounds.DEFAULT);
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
        return this.selectMap(statement, parameter, mapKey, RowBounds.DEFAULT);
    }

    @Override
    public <K, V> Map<K, V> selectMap(final String statement, final Object parameter, final String mapKey, final RowBounds rowBounds) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var18 = false;

        HashMap i$1;
        label148: {
            Map dsMap1;
            long interval3;
            try {
                var18 = true;
                if(this.isPartitioningBehaviorEnabled()) {
                    SortedMap dsMap = this.lookupDataSourcesByRouter(statement, parameter);
                    if(!MapUtils.isEmpty(dsMap)) {
                        SqlSessionCallback interval = new SqlSessionCallback() {
                            @Override
                            public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                return sqlSession.selectMap(statement,parameter, mapKey, rowBounds);
                            }
                        };

                        List originalResults = this.executeInConcurrency(interval, dsMap);
                        HashMap resultMap = new HashMap();
                        Iterator i$ = originalResults.iterator();

                        while(i$.hasNext()) {
                            Object interval1 = i$.next();
                            resultMap.putAll((Map)interval1);
                        }

                        i$1 = resultMap;
                        var18 = false;
                        break label148;
                    }
                }

                dsMap1 = super.selectMap(statement, parameter, mapKey, rowBounds);
                var18 = false;
            } finally {
                if(var18) {
                    if(this.isProfileLongTimeRunningSql()) {
                        long interval2 = System.currentTimeMillis() - startTimestamp;
                        if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                            this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                        }
                    }

                }
            }

            if(this.isProfileLongTimeRunningSql()) {
                interval3 = System.currentTimeMillis() - startTimestamp;
                if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                }
            }

            return dsMap1;
        }

        if(this.isProfileLongTimeRunningSql()) {
            long interval4 = System.currentTimeMillis() - startTimestamp;
            if(interval4 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval4)});
            }
        }

        return i$1;
    }

    @Override
    public <E> List<E> selectList(String statement) {
        return this.selectList(statement, null, RowBounds.DEFAULT);
    }

    @Override
    public <E> List<E> selectList(String statement, Object parameter) {
        return this.selectList(statement, parameter, RowBounds.DEFAULT);
    }

    @Override
    public <E> List<E> selectList(final String statement, final Object parameter, final RowBounds rowBounds) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var18 = false;

        List dsMap1;
        long interval3;
        List i$2;
        long interval4;
        label202: {
            ArrayList i$1;
            label203: {
                try {
                    var18 = true;
                    if(this.isPartitioningBehaviorEnabled()) {
                        SortedMap dsMap = this.lookupDataSourcesByRouter(statement, parameter);
                        if(!MapUtils.isEmpty(dsMap)) {
                            SqlSessionCallback interval = new SqlSessionCallback() {
                                @Override
                                public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                    return sqlSession.selectList(statement, parameter, rowBounds);
                                }
                            };

                            List originalResultList = this.executeInConcurrency(interval, dsMap);
                            if(MapUtils.isNotEmpty(this.getMergers()) && this.getMergers().containsKey(statement)) {
                                IMerger resultList = (IMerger)this.getMergers().get(statement);
                                if(resultList != null) {
                                    i$2 = (List)resultList.merge(originalResultList);
                                    var18 = false;
                                    break label202;
                                }
                            }

                            ArrayList resultList1 = new ArrayList();
                            Iterator i$ = originalResultList.iterator();

                            while(i$.hasNext()) {
                                Object interval1 = i$.next();
                                resultList1.addAll((List)interval1);
                            }

                            i$1 = resultList1;
                            var18 = false;
                            break label203;
                        }
                    }

                    dsMap1 = super.selectList(statement, parameter, rowBounds);
                    var18 = false;
                } finally {
                    if(var18) {
                        if(this.isProfileLongTimeRunningSql()) {
                            long interval2 = System.currentTimeMillis() - startTimestamp;
                            if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                            }
                        }

                    }
                }

                if(this.isProfileLongTimeRunningSql()) {
                    interval3 = System.currentTimeMillis() - startTimestamp;
                    if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                        this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                    }
                }

                return dsMap1;
            }

            if(this.isProfileLongTimeRunningSql()) {
                interval4 = System.currentTimeMillis() - startTimestamp;
                if(interval4 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval4)});
                }
            }

            return i$1;
        }

        if(this.isProfileLongTimeRunningSql()) {
            interval4 = System.currentTimeMillis() - startTimestamp;
            if(interval4 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval4)});
            }
        }

        return i$2;
    }

    @Override
    public void select(String statement, ResultHandler handler) {
        this.select(statement, null, RowBounds.DEFAULT, handler);
    }

    @Override
    public void select(String statement, Object parameter, ResultHandler handler) {
        this.select(statement, parameter, RowBounds.DEFAULT, handler);
    }

    @Override
    public void select(final String statement, final Object parameter, final RowBounds rowBounds, final ResultHandler handler) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var14 = false;

        label109: {
            try {
                var14 = true;
                if(this.isPartitioningBehaviorEnabled()) {
                    SortedMap interval = this.lookupDataSourcesByRouter(statement, parameter);
                    if(!MapUtils.isEmpty(interval)) {
                        SqlSessionCallback callback = new SqlSessionCallback() {
                            @Override
                            public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                sqlSession.select(statement, parameter, rowBounds , handler);
                                return null;
                            }
                        };

                        this.executeInConcurrency(callback, interval);
                        var14 = false;
                        break label109;
                    }
                }

                super.select(statement, parameter, rowBounds, handler);
                var14 = false;
            } finally {
                if(var14) {
                    if(this.isProfileLongTimeRunningSql()) {
                        long interval2 = System.currentTimeMillis() - startTimestamp;
                        if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                            this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                        }
                    }

                }
            }

            if(this.isProfileLongTimeRunningSql()) {
                long interval3 = System.currentTimeMillis() - startTimestamp;
                if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                }
            }

            return;
        }

        if(this.isProfileLongTimeRunningSql()) {
            long interval1 = System.currentTimeMillis() - startTimestamp;
            if(interval1 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval1)});
            }
        }
    }

    @Override
    public int insert(String statement) {
        return this.insert(statement, null);
    }

    @Override
    public int insert(final String statement, final Object parameter) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var15 = false;

        long interval1;
        int var19;
        label164: {
            int var8;
            label165: {
                int targetDataSource;
                long interval3;
                label166: {
                    try {
                        var15 = true;
                        if(!this.isPartitioningBehaviorEnabled()) {
                            targetDataSource = super.insert(statement, parameter);
                            var15 = false;
                            break label166;
                        }

                        if(parameter == null || !(parameter instanceof BatchInsertTask)) {
                            targetDataSource = 0;
                            SqlSessionCallback interval = new SqlSessionCallback() {
                                @Override
                                public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                    return sqlSession.insert(statement, parameter);
                                }
                            };
                            SortedMap resultDataSources = this.lookupDataSourcesByRouter(statement, parameter);
                            if(!MapUtils.isEmpty(resultDataSources) && resultDataSources.size() != 1) {
                                var19 = sumCollectionElement(this.executeInConcurrency(interval, resultDataSources));
                                var15 = false;
                                break label164;
                            }

                            DataSource targetDataSource1 = null;
                            if(resultDataSources.size() == 1) {
                                targetDataSource1 = (DataSource)resultDataSources.values().iterator().next();
                            }

                            var8 = (Integer)this.executeWith(targetDataSource1, interval);
                            var15 = false;
                            break label165;
                        }

                        this.logger.info("start to prepare batch insert operation with parameter type of:{}.", parameter.getClass());
                        targetDataSource = this.batchInsertAfterReordering(statement, parameter);
                        var15 = false;
                    } finally {
                        if(var15) {
                            if(this.isProfileLongTimeRunningSql()) {
                                long interval2 = System.currentTimeMillis() - startTimestamp;
                                if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                                }
                            }

                        }
                    }

                    if(this.isProfileLongTimeRunningSql()) {
                        interval3 = System.currentTimeMillis() - startTimestamp;
                        if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                            this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                        }
                    }

                    return targetDataSource;
                }

                if(this.isProfileLongTimeRunningSql()) {
                    interval3 = System.currentTimeMillis() - startTimestamp;
                    if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                        this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                    }
                }

                return targetDataSource;
            }

            if(this.isProfileLongTimeRunningSql()) {
                interval1 = System.currentTimeMillis() - startTimestamp;
                if(interval1 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval1)});
                }
            }

            return var8;
        }

        if(this.isProfileLongTimeRunningSql()) {
            interval1 = System.currentTimeMillis() - startTimestamp;
            if(interval1 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval1)});
            }
        }

        return var19;
    }

    @Override
    public int update(String statement) {
        return this.update(statement, null);
    }

    @Override
    public int update(final String statement, final Object parameter) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var16 = false;

        int i$1;
        label109: {
            int dsMap1;
            try {
                var16 = true;
                if(this.isPartitioningBehaviorEnabled()) {
                    SortedMap dsMap = this.lookupDataSourcesByRouter(statement, parameter);
                    if(!MapUtils.isEmpty(dsMap)) {
                        SqlSessionCallback interval = new SqlSessionCallback() {
                            public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                return sqlSession.update(statement,parameter);
                            }
                        };
                        List results = this.executeInConcurrency(interval, dsMap);
                        Integer rowAffacted = Integer.valueOf(0);

                        Object interval1;
                        for(Iterator i$ = results.iterator(); i$.hasNext(); rowAffacted = Integer.valueOf(rowAffacted.intValue() + ((Integer)interval1).intValue())) {
                            interval1 = i$.next();
                        }

                        i$1 = rowAffacted.intValue();
                        var16 = false;
                        break label109;
                    }
                }

                dsMap1 = super.update(statement,parameter);
                var16 = false;
            } finally {
                if(var16) {
                    if(this.isProfileLongTimeRunningSql()) {
                        long interval2 = System.currentTimeMillis() - startTimestamp;
                        if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                            this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                        }
                    }

                }
            }

            if(this.isProfileLongTimeRunningSql()) {
                long interval3 = System.currentTimeMillis() - startTimestamp;
                if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                }
            }

            return dsMap1;
        }

        if(this.isProfileLongTimeRunningSql()) {
            long interval4 = System.currentTimeMillis() - startTimestamp;
            if(interval4 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval4)});
            }
        }

        return i$1;
    }

    @Override
    public int delete(String statement) {
        return this.delete(statement, null);
    }

    @Override
    public int delete(final String statement, final Object parameter) {
        this.auditSqlIfNecessary(statement, parameter);
        long startTimestamp = System.currentTimeMillis();
        boolean var16 = false;

        int rowAffacted1;
        label137: {
            int i$1;
            label138: {
                int dsMap1;
                try {
                    var16 = true;
                    if(this.isPartitioningBehaviorEnabled()) {
                        SortedMap dsMap = this.lookupDataSourcesByRouter(statement, parameter);
                        if(!MapUtils.isEmpty(dsMap)) {
                            SqlSessionCallback interval = new SqlSessionCallback() {
                                @Override
                                public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                                    return sqlSession.delete(statement,parameter);
                                }
                            };
                            if(dsMap.size() == 1) {
                                DataSource results1 = (DataSource)dsMap.get(dsMap.firstKey());
                                rowAffacted1 = ((Integer)this.executeWith(results1, interval)).intValue();
                                var16 = false;
                                break label137;
                            }

                            List results = this.executeInConcurrency(interval, dsMap);
                            Integer rowAffacted = Integer.valueOf(0);

                            Object interval1;
                            for(Iterator i$ = results.iterator(); i$.hasNext(); rowAffacted = Integer.valueOf(rowAffacted.intValue() + ((Integer)interval1).intValue())) {
                                interval1 = i$.next();
                            }

                            i$1 = rowAffacted.intValue();
                            var16 = false;
                            break label138;
                        }
                    }

                    dsMap1 = super.delete(statement, parameter);
                    var16 = false;
                } finally {
                    if(var16) {
                        if(this.isProfileLongTimeRunningSql()) {
                            long interval2 = System.currentTimeMillis() - startTimestamp;
                            if(interval2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval2)});
                            }
                        }

                    }
                }

                if(this.isProfileLongTimeRunningSql()) {
                    long interval3 = System.currentTimeMillis() - startTimestamp;
                    if(interval3 > this.getLongTimeRunningSqlIntervalThreshold()) {
                        this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval3)});
                    }
                }

                return dsMap1;
            }

            if(this.isProfileLongTimeRunningSql()) {
                long interval4 = System.currentTimeMillis() - startTimestamp;
                if(interval4 > this.getLongTimeRunningSqlIntervalThreshold()) {
                    this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(interval4)});
                }
            }

            return i$1;
        }

        if(this.isProfileLongTimeRunningSql()) {
            long i$2 = System.currentTimeMillis() - startTimestamp;
            if(i$2 > this.getLongTimeRunningSqlIntervalThreshold()) {
                this.logger.warn("SQL Statement [{}] with parameter object [{}] ran out of the normal time range, it consumed [{}] milliseconds.", new Object[]{statement, parameter, Long.valueOf(i$2)});
            }
        }

        return rowAffacted1;
    }

    protected void auditSqlIfNecessary(final String statementName, final Object parameterObject) {
        if(this.getSqlAuditor() != null) {
            this.getSqlAuditorExecutor().execute(new Runnable() {
                public void run() {
                    CobarSqlSessionTemplate.this.getSqlAuditor().audit(statementName, CobarSqlSessionTemplate.this.getSqlByStatementName(statementName, parameterObject), parameterObject);
                }
            });
        }

    }

    protected String getSqlByStatementName(String statementName, Object parameterObject) {
        SqlSessionFactory sqlSessionFactory = (SqlSessionFactory)this.getSqlSessionFactory();
        SqlSource sqlSource = sqlSessionFactory.getConfiguration().getMappedStatement(statementName).getSqlSource();
        if(sqlSource instanceof StaticSqlSource) {
            return sqlSource.getBoundSql(parameterObject).getSql();
        } else {
            this.logger.info("dynamic sql can only return sql id.");
            return statementName;
        }
    }

    protected SortedMap<String, DataSource> lookupDataSourcesByRouter(String statementName, Object parameterObject) {
        TreeMap resultMap = new TreeMap();
        if(this.getRouter() != null && this.getCobarDataSourceService() != null) {
            List dsSet = this.getRouter().doRoute(new IBatisRoutingFact(statementName, parameterObject)).getResourceIdentities();
            if(CollectionUtils.isNotEmpty(dsSet)) {
                Collections.sort(dsSet);
                Iterator i$ = dsSet.iterator();

                while(i$.hasNext()) {
                    String dsName = (String)i$.next();
                    resultMap.put(dsName, this.getCobarDataSourceService().getDataSources().get(dsName));
                }
            }
        }

        return resultMap;
    }

    protected Object executeWith(DataSource dataSource, SqlSessionCallback action) {
        SqlSession session = null;
        boolean isRealRequireClosedConnection = true;
        Object t;
        try {
            Connection springCon = null;
            boolean transactionAware = dataSource instanceof TransactionAwareDataSourceProxy;

            try {
                springCon = transactionAware?dataSource.getConnection(): DataSourceUtils.doGetConnection(dataSource);
                session = this.getSqlSessionFactory().openSession(getExecutorType(),springCon);
            } catch (SQLException var29) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", var29);
            }

            try {
                t = action.doInSqlSession(session);
            } catch (SQLException var26) {
                t = var26;
                throw (new SQLErrorCodeSQLExceptionTranslator()).translate("SqlMapClient operation", (String)null, var26);
            } catch (Throwable var27) {
                t = var27;
                throw new UncategorizedCobarClientException("unknown excepton when performing data access operation.", var27);
            } finally {
                try {
                    if(springCon != null) {
                        if(transactionAware) {
                            springCon.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(springCon, dataSource);
                        }
                        isRealRequireClosedConnection = springCon.isClosed();
                    }
                } catch (Throwable var25) {
                    this.logger.debug("Could not close JDBC Connection", var25);
                }

            }
        } finally {
            if (isRealRequireClosedConnection && session != null) {
                session.close();
            }
        }

        return t;
    }

    public List<Object> executeInConcurrency(SqlSessionCallback action, SortedMap<String, DataSource> dsMap) {
        ArrayList requests = new ArrayList();
        Iterator results = dsMap.entrySet().iterator();

        while(results.hasNext()) {
            Map.Entry entry = (Map.Entry)results.next();
            ConcurrentRequest request = new ConcurrentRequest();
            request.setAction(action);
            request.setDataSource((DataSource)entry.getValue());
            request.setExecutor((ExecutorService)this.getDataSourceSpecificExecutors().get(entry.getKey()));
            requests.add(request);
        }

        List results1 = this.getConcurrentRequestProcessor().process(requests);
        return results1;
    }

    private int batchInsertAfterReordering(final String statementName, final Object parameterObject) {
        HashSet keys = new HashSet();
        keys.add(this.getDefaultDataSourceName());
        keys.addAll(this.getCobarDataSourceService().getDataSources().keySet());
        final CobarMRBase mrbase = new CobarMRBase(keys);
        ExecutorService executor = this.createCustomExecutorService(Runtime.getRuntime().availableProcessors(), "batchInsertAfterReordering");

        try {
            final StringBuffer requests = new StringBuffer();
            Collection i$ = ((BatchInsertTask)parameterObject).getEntities();
            final CountDownLatch entity = new CountDownLatch(i$.size());
            Iterator paramList = i$.iterator();

            while(paramList.hasNext()) {
                final Object identity = paramList.next();
                Runnable dataSourceToUse = new Runnable() {
                    public void run() {
                        try {
                            SortedMap t = CobarSqlSessionTemplate.this.lookupDataSourcesByRouter(statementName, identity);
                            if(MapUtils.isEmpty(t)) {
                                CobarSqlSessionTemplate.this.logger.info("can\'t find routing rule for {} with parameter {}, so use default data source for it.", statementName, identity);
                                mrbase.emit(CobarSqlSessionTemplate.this.getDefaultDataSourceName(), identity);
                            } else {
                                if(t.size() > 1) {
                                    throw new IllegalArgumentException("unexpected routing result, found more than 1 target data source for current entity:" + identity);
                                }

                                mrbase.emit((String)t.firstKey(), identity);
                            }
                        } catch (Throwable var5) {
                            requests.append(ExceptionUtils.getFullStackTrace(var5));
                        } finally {
                            entity.countDown();
                        }

                    }
                };
                executor.execute(dataSourceToUse);
            }

            try {
                entity.await();
            } catch (InterruptedException var16) {
                throw new ConcurrencyFailureException("unexpected interruption when re-arranging parameter collection into sub-collections ", var16);
            }

            if(requests.length() > 0) {
                throw new ConcurrencyFailureException("unpected exception when re-arranging parameter collection, check previous log for details.\n" + requests);
            }
        } finally {
            executor.shutdown();
        }

        ArrayList requests1 = new ArrayList();
        Iterator i$1 = mrbase.getResources().entrySet().iterator();

        while(i$1.hasNext()) {
            Map.Entry entity1 = (Map.Entry)i$1.next();
            final List paramList1 = (List)entity1.getValue();
            if(!CollectionUtils.isEmpty(paramList1)) {
                String identity1 = (String)entity1.getKey();
                DataSource dataSourceToUse1 = this.findDataSourceToUse(identity1);
                SqlSessionCallback callback = new SqlSessionCallback() {
                    @Override
                    public Object doInSqlSession(SqlSession sqlSession) throws SQLException {
                        return sqlSession.insert(statementName, paramList1);
                    }
                };
                ConcurrentRequest request = new ConcurrentRequest();
                request.setDataSource(dataSourceToUse1);
                request.setAction(callback);
                request.setExecutor((ExecutorService)this.getDataSourceSpecificExecutors().get(identity1));
                requests1.add(request);
            }
        }

        return sumCollectionElement(this.getConcurrentRequestProcessor().process(requests1));
    }

    private DataSource findDataSourceToUse(String key) {
        DataSource dataSourceToUse = null;
        if(StringUtils.equals(key, this.getDefaultDataSourceName())) {
            dataSourceToUse = getDefaultDataSource();
        } else {
            dataSourceToUse = (DataSource)this.getCobarDataSourceService().getDataSources().get(key);
        }

        return dataSourceToUse;
    }

    public static int sumCollectionElement(Collection<Object> coll) {
        int result = 0;
        for (Iterator<?> iter = coll.iterator(); iter.hasNext();) {
            result += (Integer) iter.next();
        }
        return result;
    }

}
