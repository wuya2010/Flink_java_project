package com.alibaba.bean;

import lombok.Data;

import java.sql.Timestamp;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/10 8:48
 */

@Data
public class testObj {
    String engine_name;
    int device_type;
    String cost_name;
    float cost_value;
    Timestamp last_update;
    String comment;
}
