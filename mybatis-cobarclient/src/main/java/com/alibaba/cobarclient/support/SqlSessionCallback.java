package com.alibaba.cobarclient.support;

import org.apache.ibatis.session.SqlSession;

import java.sql.SQLException;


public interface SqlSessionCallback {

    Object doInSqlSession(SqlSession sqlSession) throws SQLException;
}
