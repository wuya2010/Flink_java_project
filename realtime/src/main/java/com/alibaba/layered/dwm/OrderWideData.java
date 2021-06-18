package com.alibaba.layered.dwm;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/10 11:33
 */
public class OrderWideData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);




    }
}
