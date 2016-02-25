package com.cobar.demo.service;

import com.cobar.demo.dao.TradeMapper;
import com.cobar.demo.dao.UserMapper;
import com.cobar.demo.model.TradeModel;
import com.cobar.demo.model.UserModel;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Description:
 * User: ouzhouyou@raycloud.com
 * Date: 14-6-14
 * Time: 下午2:52
 * Version: 1.0
 */
@Service
public class UserService {

    @Autowired
    UserMapper userMapper;
    @Autowired
    TradeMapper tradeMapper;
    @Autowired
    private PlatformTransactionManager txManager;

    public List<UserModel> getUserList() {
        return userMapper.getUserList();
    }

    public void insertUserNotTransaction(UserModel userModel) {
        userMapper.insertUser(userModel);
        TradeModel tradeModel = new TradeModel();
        tradeModel.setTid(10086L);
        tradeModel.setTitle("手动事务测试");
        tradeModel.setSplitDBName("1");
        tradeMapper.insertTrade(tradeModel);
    }

    public void insertUser(UserModel userModel) {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        TransactionStatus status = txManager.getTransaction(def);
        try {
            userMapper.insertUser(userModel);
            TradeModel tradeModel = new TradeModel();
            tradeModel.setTid(10086L);
            tradeModel.setTitle("手动事务测试");
            tradeModel.setSplitDBName("1");
            tradeMapper.insertTrade(tradeModel);
            txManager.commit(status);
        } catch (Exception e) {
            e.printStackTrace();
            txManager.rollback(status);
        }
    }

    public void insertUserTemplate(final UserModel userModel) {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        TransactionTemplate transactionTemplate = new TransactionTemplate(txManager,def);
        transactionTemplate.execute(new TransactionCallback() {
            public Object doInTransaction(TransactionStatus arg0) {
                userMapper.insertUser(userModel);
                TradeModel tradeModel = new TradeModel();
                tradeModel.setTid(10086L);
                tradeModel.setTitle("注解事务测试");
                tradeModel.setSplitDBName("1");
                tradeMapper.insertTrade(tradeModel);
                return null;
            }
        });
    }

    @Transactional
    public void insertUserAnnotations(UserModel userModel) {
        userMapper.insertUser(userModel);
        TradeModel tradeModel = new TradeModel();
        tradeModel.setTid(10086L);
        tradeModel.setTitle("注解事务测试");
        tradeModel.setSplitDBName("1");
        tradeMapper.insertTrade(tradeModel);
    }

    public void clean(String userNick) {
        userMapper.deleteUser(userNick);
        tradeMapper.deleteTrade(10086L,"1");
    }
}
