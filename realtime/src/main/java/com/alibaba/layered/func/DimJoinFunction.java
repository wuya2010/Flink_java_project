package com.alibaba.layered.func;

import com.alibaba.fastjson.JSONObject;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/18 11:53
 */

//设置泛型
public interface DimJoinFunction<T> {

    String getKey(T obj);

    void join(T obj , JSONObject dimInfoJson) throws Exception;
}
