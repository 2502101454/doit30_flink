package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author zengwang
 * @create 2023-09-23 16:35
 * @desc:
 */
public class Demo6_Temporary_TableView {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 环境创建之处，会地总初始化一个 元数据空间实现对象(defalut_catalog   ==> GenericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        String hiveConfDir = "E:\\IDEAProjects\\doit30_flink\\conf\\flink_conf\\";
        HiveCatalog hiveCatalog = new HiveCatalog("myhive", null, hiveConfDir = hiveConfDir);
        // 注册hive catalog, catalogManager就可以打印出来它们(即register时的名称)
        tenv.registerCatalog("wz_hiveCat", hiveCatalog);
        tenv.executeSql(
                "create temporary table wz_hiveCat.doit.t_kafka                                  "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'doit30-3',                              "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        tenv.executeSql("create temporary view wz_hiveCat.doit.t_kafka_view as select id, name from wz_hiveCat.doit.t_kafka");
        tenv.listCatalogs();
    }
}
