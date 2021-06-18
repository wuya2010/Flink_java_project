package com.alibaba.utils;


import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 14:34
 */
public class DateTimeUtil {

    public static final DateTimeFormatter  df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 返回时间戳
     * @param dateStr
     * @return
     */
    public static Long toTs(String dateStr){
        LocalDateTime parse = LocalDateTime.parse(dateStr, df);
        long ts = parse.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }
}
