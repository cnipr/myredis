package com.cnipr.myredis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import redis.clients.jedis.Jedis;

public class App2 {
	public static void main(String[] args) {
		String[] tableNames = {
//				"patent_after_2007_01_341998",
//				"patent_after_2007_02_265198",
//				"patent_after_2007_03_416290",
//				"patent_after_2007_04_461220",
//				"patent_after_2007_05_242979",
//				"patent_after_2007_06_513700",
//				"patent_after_2007_07_86300",
//				"patent_after_2007_08_30w",
//				"patent_after_2007_09_379137",
//				"patent_after_2007_10_20863",
				
//				"patent_after_2007_11_30w",
//				"patent_after_2007_12_30w",
				
				"patent_after_2007_13_30w",
				"patent_after_2007_14_30w",
				"patent_after_2007_15_30w",
				"patent_after_2007_16_30w",
				"patent_after_2007_17_30w",
				"patent_after_2007_18_30w",
				"patent_after_2007_19_30w",
				"patent_after_2007_20_30w",
				"patent_after_2007_21_30w",
				"patent_after_2007_22_30w",
				"patent_after_2007_23_30w",
				"patent_after_2007_24_30w",
				"patent_after_2007_25_30w",
				"patent_after_2007_26_30w",
				"patent_after_2007_27_30w",
				"patent_after_2007_28_30w",
				"patent_after_2007_29_30w",
				"patent_after_2007_30_30w",
				"patent_after_2007_31_30w",
				"patent_after_2007_32_30w",
				"patent_after_2007_33_30w",
				"patent_after_2007_34_30w",
				"patent_after_2007_35_30w",
				"patent_after_2007_36_30w",
				"patent_after_2007_37_30w",
				"patent_after_2007_38_30w",
				"patent_after_2007_39_30w",
				"patent_after_2007_40_30w",
				"patent_after_2007_41_30w",
				"patent_after_2007_42_30w",
				"patent_after_2007_43_30w",
				"patent_after_2007_44_30w",
				"patent_after_2007_45_30w",
				"patent_after_2007_46_30w",
				"patent_after_2007_47_30w",
				"patent_after_2007_48_30w",
				"patent_after_2007_49_30w",
				"patent_after_2007_50_30w",
				"patent_after_2007_51_30w",
				"patent_after_2007_52_30w",
				"patent_after_2007_53_30w",
				"patent_after_2007_54_30w",
				"patent_after_2007_55_30w",
				"patent_after_2007_56_30w",
				"patent_after_2007_57_30w"};
		for (int i = 0; i < tableNames.length; i++) {
			String tableName = tableNames[i];
			etlShencha(tableName);			
		}
	}

	private static void etlShencha(String tableName) {
		System.out.println(new Date() + ",start:" + tableName);
		long start = System.currentTimeMillis();
		Jedis jedis = new Jedis("192.168.242.130", 6379);
		jedis.auth("foobared");
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String url = "jdbc:mysql://127.0.0.1/data?useUnicode=true&characterEncoding=utf-8";
			Connection con = DriverManager.getConnection(url, "root", "root");
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery("select id,shencha from " + tableName);
			int i = 0;
			while (rs.next()) {
				int id = rs.getInt(1);
				String an = rs.getString(2);
				if (an == null) {
					continue;
				}
				jedis.set(id + "", an);
				i++;
				if (i % 10000 == 0) {
					System.out.println(new Date() + "," + i);
				}
			}
			rs.close();
			st.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long consume = System.currentTimeMillis() - start;
		System.out.println("耗时：" + consume);
	}
}
