package com.alibaba.layered.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.DimUtil;
import com.alibaba.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;
import scala.collection.Iterable;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;


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
//public abstract  class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{
//
//    private ExecutorService pool;
//
//    //给出表名
//    private String tableName;
//
//    //构造器
//    public DimAsyncFunction(String tableName){
//        this.tableName = tableName;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        System.out.println("初始化线程池对象");
//        pool = ThreadPoolUtil.getInstance();
//    }
//
//
//    /**
//     * 自定义一个线程，对数据进行聚合操作,怎么实现异步聚合
//     * @param input
//     * @param resultFuture
//     */
//    @Override
//    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
//
//        pool.submit(
//                new Runnable() {
//                    @Override
//                    public void run() {
//                        try {
//                            // 1. 获取聚合的时间差 ； 2. 提取表的 id  进行聚合
//                            long start_timeStmap = System.currentTimeMillis();
//
//                            // 获取流数据的 key
//                            String key = getKey(input);
//
//                            //异步查找 从hbase 获取对应的key
//                            JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
//
//                            //如果查找到的数据不为空
//                            if(dimInfo != null ){
//                                join(input,dimInfo);
//                            }
//
//                            long end_timeStmap = System.currentTimeMillis();
//                            System.out.println("join time is "+ (end_timeStmap - start_timeStmap));
//
////                            resultFuture.complete(Arrays.asList(input));
//
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                            throw new RuntimeException("something");
//                        }
//                    }
//                }
//        );
//    }
//
//}



public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;

    //维度的表名
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    /**
     * 发送异步请求的方法
     * @param obj     流中的事实数据
     * @param resultFuture      异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) {
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //发送异步请求
                            long start = System.currentTimeMillis();
                            //从流中事实数据获取key
                            String key = getKey(obj);

                            //根据维度的主键到维度表中进行查询
                            JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                            //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                            if(dimInfoJsonObj != null){
                                //维度关联  流中的事实数据和查询出来的维度数据进行关联
                                join(obj,dimInfoJsonObj);
                            }
                            //System.out.println("维度关联后的对象:" + obj);
                            long end = System.currentTimeMillis();
                            System.out.println("异步维度查询耗时" +(end -start)+"毫秒");
                            //将关联后的数据数据继续向下传递
//                            resultFuture.complete(Arrays.asList(obj));

                            // fixme: 转换为 Iterable
                            resultFuture.complete((Iterable) Arrays.asList(obj).iterator());

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(tableName + "维度异步查询失败");
                        }
                    }
                }
        );
    }
}

