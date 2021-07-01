package com.alibaba.layered.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 * 基于测试服务器
 * @author kylin
 * @version 1.0
 * @date 2021/6/24 10:33
 */

 // 定义返回的数据类型
@FunctionHint(output=@DataTypeHint("ROW<ct BIGINT,sour STRING>"))
public class KeywordProductC2RUDTF extends TableFunction<Row> {

    //传入参数： Long clickCt, Long cartCt, Long orderCt
    public void eval(Long clickCt, Long cartCt, Long orderCt){

    }


}
