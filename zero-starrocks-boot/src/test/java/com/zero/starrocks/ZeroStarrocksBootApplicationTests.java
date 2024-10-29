package com.zero.starrocks;

import com.zero.starrocks.common.util.MysqlClientUtil;
import com.zero.starrocks.common.util.StarRocksStreamLoadUtil;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@SpringBootTest
class ZeroStarrocksBootApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    void testMysqlClient() {
        String host = "127.0.0.1";
        //query_port in fe.conf
        String port = "9030";
        String user = "root";
        //password is empty by default
        String password = "";

        //connect to starrocks
        Connection conn = null;
        try {
            conn = MysqlClientUtil.getConn(host, port, user, password, "");
        } catch (Exception e) {
            System.out.println("connect to starrocks failed");
            e.printStackTrace();
            return;
        }
        System.out.println("connect to starrocks successfully");

        //create statement
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            System.out.println("create statement failed");
            e.printStackTrace();
            MysqlClientUtil.closeConn(conn);
            return;
        }
        System.out.println("create statement successfully");

        //create database
        try {
            stmt.execute("CREATE DATABASE IF NOT EXISTS zero");
        } catch (SQLException e) {
            System.out.println("create database failed");
            e.printStackTrace();
            MysqlClientUtil.closeStmt(stmt);
            MysqlClientUtil.closeConn(conn);
            return;
        }
        System.out.println("create database successfully");

        //set db context
        try {
            stmt.execute("USE zero");
        } catch (SQLException e) {
            System.out.println("set db context failed");
            e.printStackTrace();
            MysqlClientUtil.closeStmt(stmt);
            MysqlClientUtil.closeConn(conn);
            return;
        }
        System.out.println("set db context successfully");

        //create table
        try {
            stmt.execute("CREATE TABLE IF NOT EXISTS system_user(siteid INT, citycode SMALLINT, pv BIGINT SUM) " +
                    "AGGREGATE KEY(siteid, citycode) " +
                    "DISTRIBUTED BY HASH(siteid) BUCKETS 10 " +
                    "PROPERTIES(\"replication_num\" = \"1\")");
        } catch (Exception e) {
            System.out.println("create table failed");
            e.printStackTrace();
            MysqlClientUtil. closeStmt(stmt);
            MysqlClientUtil.closeConn(conn);
            return;
        }
        System.out.println("create table successfully");

        //insert data
        try {
            stmt.execute("INSERT INTO system_user values(1, 2, 3), (4, 5, 6), (1, 2, 4)");
        } catch (Exception e) {
            System.out.println("insert data failed");
            e.printStackTrace();
            MysqlClientUtil.closeStmt(stmt);
            MysqlClientUtil.closeConn(conn);
            return;
        }
        System.out.println("insert data successfully");

        //query data
        try {
            ResultSet result = stmt.executeQuery("SELECT * FROM system_user");
            System.out.println("data queried is :");
            while (result.next()) {
                int siteid = result.getInt("siteid");
                int citycode = result.getInt("citycode");
                int pv = result.getInt("pv");
                System.out.println("\t" + siteid + "\t" + citycode + "\t" + pv);
            }
        } catch (Exception e) {
            System.out.println("query data failed");
            e.printStackTrace();
            MysqlClientUtil.closeStmt(stmt);
            MysqlClientUtil.closeConn(conn);
            return;
        }

        //drop database
//        try {
//            stmt.execute("DROP DATABASE IF EXISTS zero");
//        } catch (Exception e) {
//            System.out.println("drop database failed");
//            e.printStackTrace();
//            MysqlClientUtil.closeStmt(stmt);
//            MysqlClientUtil.closeConn(conn);
//            return;
//        }
        System.out.println("drop database successfully");
        MysqlClientUtil.closeStmt(stmt);
        MysqlClientUtil.closeConn(conn);
    }

    @Test
    void testStarRocksStreamLoad() throws Exception {
        int id1 = 1;
        int id2 = 10;
        String id3 = "Simon";
        int rowNumber = 10;
        String oneRow = id1 + "\t" + id2 + "\t" + id3 + "\n";

        StringBuilder stringBuilder = new StringBuilder();
//        for (int i = 0; i < rowNumber; i++) {
//            stringBuilder.append(oneRow);
//        }
        stringBuilder.append(oneRow.repeat(rowNumber));

        stringBuilder.deleteCharAt(stringBuilder.length() - 1);

        String loadData = stringBuilder.toString();
        StarRocksStreamLoadUtil starrocksStreamLoadUtil = new StarRocksStreamLoadUtil();
        starrocksStreamLoadUtil.sendData(loadData);
    }

}
