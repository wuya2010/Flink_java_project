package com.alibaba.layered.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.DimUtil;
import com.alibaba.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/18 11:47
 * 
 * 获取全量数据
 */
//实现部分抽象方法
public abstract  class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    private ExecutorService pool;

    //给出表名
    private String tableName;

    //构造器
    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("初始化线程池对象");
        pool = ThreadPoolUtil.getInstance();
    }


    /**
     * 自定义一个线程，对数据进行聚合操作,怎么实现异步聚合
     * @param input
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {

        pool.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 1. 获取聚合的时间差 ； 2. 提取表的 id  进行聚合
                            long start_timeStmap = System.currentTimeMillis();

                            // 获取流数据的 key
                            String key = getKey(input);

                            //异步查找 从hbase 获取对应的key
                            JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);

                            //如果查找到的数据不为空
                            if(dimInfo != null ){
                                join(input,dimInfo);
                            }

                            long end_timeStmap = System.currentTimeMillis();
                            System.out.println("join time is "+ (end_timeStmap - start_timeStmap));

//                            resultFuture.complete(Arrays.asList(input));

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("something");
                        }
                    }
                }
        );
    }

}
