package com.alibaba.cobarclient.support.execution;


import java.util.List;

/**
 * Created by wuzhong on 2016/2/24.
 */
public interface IConcurrentRequestProcessor {

    List<Object> process(List<ConcurrentRequest> var1);
}
