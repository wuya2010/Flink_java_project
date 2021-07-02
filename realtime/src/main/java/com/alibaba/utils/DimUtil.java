package com.alibaba.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.TableName;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Executable;
import java.util.List;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/18 14:54
 */
public class DimUtil {

    /**
     * 根据 表名 + key 获取数据
     * @param tableName
     */
    //在做维度关联的时候，大部分场景都是通过id进行关联，所以提供一个方法，只需要将id的值作为参数传进来即可
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {
        /*
           1. 从redis查找数据，如果redis中没有，再从 hbase 中获取，并将数据缓存放入redis
           2. 构建查询语句
         */
        String querySql = " where ";
        String redisKey = "dim:"+tableName.toLowerCase() + ":"; //dim:test_bigdata:
        //编列列
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple = cloNameAndValue[i];
            String fieldName = tuple.f0;
            String fieldValue = tuple.f1;
            //对于大于0的单独进行拼接
            if(i>0){
                querySql += " and ";
                redisKey += "_";
            }
            querySql += fieldName + "='" + fieldValue + "'"; // where id = '1' and xx='2'
            redisKey += fieldValue; // dim:test_bigdata:1_2_3
        }

        // 分别从 redis / hbase  获取数据
        //从Redis中获取数据
        Jedis jedis = null;
        //维度数据的json字符串形式
        String redis_hbase_str = null;
        //维度数据的json对象形式
        JSONObject redis_hbase_obj = null;

        //还需要关闭redis
        try {
            jedis = RedisUtil.getJedis();
            redis_hbase_str = jedis.get(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("cannot find data from redis");
        }

        //判断redis获取的数据是否为空
        if(redis_hbase_obj != null && redis_hbase_str.length() > 0){
            redis_hbase_obj = JSONObject.parseObject(redis_hbase_str);
        }else{
            //redis 无法获取，则从hbase 获取数据,并将最终结果写入到 redis
            String hbaseSql = "select * from " + tableName + querySql; //查询语句
            List<JSONObject> dimList = PhoneixUtil.queryList(hbaseSql,JSONObject.class); //返回值的类型

            if(dimList != null && dimList.size() > 0){
                redis_hbase_obj = dimList.get(0);
                if(redis_hbase_obj != null){
                    // Setex 命令为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值
                    jedis.setex(redisKey,3600 * 24, redis_hbase_obj.toString());
                }
            }else{
                System.out.println("cannot find data from hbase");
            }
        }

        if(jedis != null){
            jedis.close();
        }

        return redis_hbase_obj;

    }

    // 没有使用缓存 , 获取数据
    public static JSONObject getDimInfoNoCache(String tableName,  Tuple2<String, String>... cloNameAndValue){
        return null;
    }

    //根据key让Redis中的缓存失效
    public static void deleteCache(String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try{
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key);
            jedis.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //测试获取数据
    public static void main(String[] args) {

    }


}
