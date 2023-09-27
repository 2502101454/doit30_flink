package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author zengwang
 * @create 2023-09-23 16:35
 * @desc:
 */
public class Demo5_CatalogDemo {
    public static void main(String[] args) {
        // 创建hive表=创建hdfs文件，不设置就报hdfs权限问题，默认取本机用户名zengw，没有hdfs访问权限问题
        // 涛哥、官网教程中没这么设置，估计人家本地环境都ok
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 环境创建之处，会地总初始化一个 元数据空间实现对象(defalut_catalog   ==> GenericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // tenv.listCatalogs();
        tenv.executeSql("show catalogs").print();
        // 切换catalog时，加上关键字catalog，不然就认为在切换db
        tenv.executeSql("use catalog default_catalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use default_database");
        tenv.executeSql("show tables").print();

        System.out.println("===============================");
        String name = "myhive";
        String defaultDatabase = null;
        String hiveConfDir = "E:\\IDEAProjects\\doit30_flink\\conf\\flink_conf\\";
        HiveCatalog hiveCatalog = new HiveCatalog(name = name, defaultDatabase = defaultDatabase, hiveConfDir = hiveConfDir);
        // 注册hive catalog, catalogManager就可以打印出来它们(即register时的名称)
        tenv.registerCatalog("wz_hiveCat", hiveCatalog);
        tenv.executeSql("show catalogs").print();
        // 注册不代表使用，切换为hive的catalog
        tenv.useCatalog("wz_hiveCat");
        tenv.executeSql("create database if not exists doit");
        tenv.executeSql("show databases").print();
        tenv.useDatabase("doit");
        tenv.executeSql(
                "create table t_kafka                                  "
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
        tenv.executeSql("show tables").print();

    }
}
