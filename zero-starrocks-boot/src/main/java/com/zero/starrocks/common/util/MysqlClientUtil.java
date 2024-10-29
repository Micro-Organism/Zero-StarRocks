/**
Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
		"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
**/

package com.zero.starrocks.common.util;

import java.sql.*;

public class MysqlClientUtil {

    public static Connection getConn(String host, String port, String user, String password, String database) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + user + "&password=" + password;
        return DriverManager.getConnection(url);
    }

    public static void closeConn(Connection conn) {
        try {
            conn.close();
            System.out.println("conn closed");
        } catch (Exception e) {
            System.out.println("close conn failed");
            e.printStackTrace();
        }
    }

    public static void closeStmt(Statement stmt) {
        try {
            stmt.close();
            System.out.println("stmt closed");
        } catch (Exception e) {
            System.out.println("close stmt failed");
            e.printStackTrace();
        }
    }
}