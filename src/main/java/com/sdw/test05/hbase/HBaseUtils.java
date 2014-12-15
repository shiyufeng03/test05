package com.sdw.test05.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.springframework.core.io.ClassPathResource;

public class HBaseUtils {
	public static final String CHARSETNAME = "UTF-8";
	private final static String HBaseSiteKey = "hbase-site.xml";
	private static Configuration hbaseConfig;
	private final static Object lock = new Object();

	public static Configuration getConf() {
		if (hbaseConfig == null) {
			synchronized (lock) {
				if (hbaseConfig == null) {
					ClassPathResource path = new ClassPathResource(HBaseSiteKey);
					hbaseConfig = new Configuration();
					hbaseConfig.addResource(path.getFilename());
				}
			}
		}
		return hbaseConfig;
	}

	private static HTablePool hTablePool;
	private final static int TABLE_REF_MAX_SIZE = 100;

	public static HTablePool getHTablePool() {
		if (hTablePool == null) {
			synchronized (lock) {
				if (hTablePool == null) {
					hTablePool = new HTablePool(getConf(), TABLE_REF_MAX_SIZE);
				}
			}
		}

		return hTablePool;
	}
}
