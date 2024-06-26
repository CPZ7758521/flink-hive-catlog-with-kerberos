package com.pandora.www;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Flink_hive_catalog_with_kerberos {
    static Configuration conf;
    static String hadoopConfPath = "";
    static String hiveConfPath = "";

    private static Logger LOG = LoggerFactory.getLogger(Flink_hive_catalog_with_kerberos.class);

    public static void main(String[] args) {
        try {
//            System.setProperty("https.protocols", "TLSv1.1,TLSv1.2");
//            System.setProperty("jdk.tls.client.protocols", "TLSv1.1,TLSv1.2");
            System.setProperty("java.security.krb5.conf", hadoopConfPath + "krb5.conf");

            conf = new Configuration();
//            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//            conf.setBoolean("fs.hdfs.impl.disable.cache", true);

//            conf.addResource(hadoopConfPath + "core-site.xml");
//            conf.addResource(hadoopConfPath + "hdfs-site.xml");
//            conf.addResource(hiveConfPath + "hive-site.xml");

            //需要打开下边的配置，要不然是本地用户
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("test/dev@TEST.CPZ.COM", hadoopConfPath + "test.keytab");

            LOG.info("Current User is : " + UserGroupInformation.getCurrentUser());
            LOG.info("Kerberos认证成功！！");
        } catch (IOException e) {
            LOG.error("kerberos login failure" + e.getMessage());
            e.printStackTrace();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String catalogName = "flink";
        String defaultDatabase = "test";
        String hiveConfDir = hiveConfPath;
        String hadoopConfDir = hadoopConfPath;
        String hiveVersion = "2.1.1";

        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, hadoopConfDir, hiveVersion);
        tableEnv.registerCatalog(catalogName, hiveCatalog);

        // set the hivecatalog as the current catalog of the session
        tableEnv.useCatalog(catalogName);

        String sql = "select * from test.testa";
        tableEnv.executeSql(sql);

    }
}
