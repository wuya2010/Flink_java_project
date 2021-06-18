package com.alibaba.bean;

import lombok.Data;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/9 14:50
 */
@Data //自动载入 get/set 方法 , 需要在 idea 安装插件，否则无法生成
public class TableProcess {
    //实现动态分流
    public static final String SINK_TYEP_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    //变量
    String soureTable;
    String operateType;
    String sinkType;
    String sinkTable;
    String sinkPk; //主键字段
    String sinkExtend; //建表扩展
    String sinkColumns;

    public static void main(String[] args) {
        TableProcess tableProcess = new TableProcess();
        tableProcess.getOperateType();
    }
}
