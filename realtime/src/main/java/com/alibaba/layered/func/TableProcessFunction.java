package com.alibaba.layered.func;


import com.alibaba.bean.TableProcess;
import com.alibaba.common.ConstantConf;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.MySqlUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/9 15:15
 */
//输入输出数据类型： <I, O>
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    //hbase 数据
    private Map<String, TableProcess> objMap = new HashMap<String, TableProcess>();

    //已经处理过的表，存放表名
    private Set<String> existsTablesSet = new HashSet<String>();

    //建立 phoenix 的连接
    Connection conn = null;

    public TableProcessFunction(OutputTag<JSONObject> outputTag){
        this.outputTag = outputTag;
    }

    //建立hbase 的连接
    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(ConstantConf.PHOENIX_SERVER);

        //初始表信息
        refreshMeta();

        //定时任务，查找mysql数据，获取map 的变化
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 5000, 5000);

    }

    /**
     * 对每一条数据的处理
     * @param jsonObj
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
            /*  1.从 jsonObj 中获取 table, type 信息，索引map中对应的 processTable 中的 sink_table, sinkColumn 字段
                2.需要对保留字段进行进行过滤，判断sinkColumn 中的是否有保留字段, 包含在样例类中的字段信息保留
                3. 根据数据类型，分别写入 habse / kafka
             */

    }





    /**
     * 定义辅助方法: 更新数据
     */
    private void refreshMeta() {
        List<TableProcess> resultProcess = MySqlUtil.queryList("", TableProcess.class, true);
        //遍历list的结果
        for (TableProcess process : resultProcess) {
            String soureTable = process.getSoureTable();
            // update / insert ...
            String operateType = process.getOperateType();

            //输出类型 kafka / hbase
            String sinkType = process.getSinkType();
            String sinkTable = process.getSinkTable();
            String sinkColumns = process.getSinkColumns();
            //表的主键
            String sinkPk = process.getSinkPk();
            //建表扩展语句
            String sinkExtend = process.getSinkExtend();

            //拼接 key
            String key = soureTable + ":" + operateType;

          /*
            获取表中所有的信息，放在map 中 （k:主键， v: object） ，
             判断表的写入方式，如果是insert并且type为hbaes, pheneix 是否存在表
           */
            objMap.put(key, process);

            if (TableProcess.SINK_TYEP_HBASE.equals(sinkType) && "insert".equals(operateType)) {
                //添加元素并返回，如果不包含返回 true
                boolean notExists = existsTablesSet.add(soureTable);
                if (notExists) {
                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }
            }
        }
        if(objMap == null || objMap.size() == 0 ){
            throw new RuntimeException("not read data from mysql data");
        }

    }

    /**
     * 检查表是否存在，没有就创建新表
     */
    private void checkTable(String tableName, String fields, String kw, String extend) {
        //如果在配置表中，没有配置主键 需要给一个默认主键的值
        if (kw == null) {
            kw = "id";
        }
        //如果在配置表中，没有配置建表扩展 需要给一个默认建表扩展的值
        if (extend == null) {
            extend = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                ConstantConf.HBASE_SCHEMA + "." + tableName + "(");

        //对建表字段进行切分
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            //判断当前字段是否为主键字段
            if (kw.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append("info.").append(field).append(" varchar ");
            }
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(extend);

        System.out.println("crate table by sql");

        // import java.sql.PreparedStatement;
        PreparedStatement ps = null;

        //建立 hbase 的 连接
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    throw new RuntimeException("create table failed");
                }
            }
        }
    }

    /**
     * 对 Data 中的数据过滤
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

    }

}
