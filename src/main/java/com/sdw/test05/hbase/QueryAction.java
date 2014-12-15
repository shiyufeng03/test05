package com.sdw.test05.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class QueryAction {

    public static void main(String[] args) {
        readUsergrowthAccount();
        readNonexistColumn();
    }

    private static void readUsergrowthAccount() {
        HTableInterface hTable = null;
        try {
            HTablePool pool = HBaseUtils.getHTablePool();
            hTable = pool.getTable("usergrowth_account");
            Get get = new Get(Bytes.toBytes("02431579123"));
            get.addFamily(Bytes.toBytes("cp"));
            get.setMaxVersions(10);

            Result result = hTable.get(get);
            if (result.isEmpty()) {
                System.out.println("empty");
            }
            for (KeyValue kv : result.list()) {
                String family = Bytes.toString(kv.getFamily());
                String qualifier = Bytes.toString(kv.getQualifier());
                byte[] value = kv.getValue();
                if (value != null && value.length > 0) {
                    System.out.println(qualifier);
                    
                }
            }
            pool.putTable(hTable);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if (hTable != null) {
                try {
                    hTable.close();
                } catch (IOException e1) {

                }
            }
        }
    }
    
    private static void readNonexistColumn() {
        HTableInterface hTable = null;
        try {
            HTablePool pool = HBaseUtils.getHTablePool();
            hTable = pool.getTable("usergrowth_account");
            Get get = new Get(Bytes.toBytes("02431579123"));
            //get.addFamily(Bytes.toBytes("cp"));
            get.addColumn(Bytes.toBytes("cp"), Bytes.toBytes("cp"));
            get.setMaxVersions(10);

            Result result = hTable.get(get);
            if (result.isEmpty()) {
                System.out.println("empty");
                return;
            }
            for (KeyValue kv : result.list()) {
                String family = Bytes.toString(kv.getFamily());
                String qualifier = Bytes.toString(kv.getQualifier());
                byte[] value = kv.getValue();
                if (value != null && value.length > 0) {

                    System.out.println("columnName:" + qualifier);
                }
            }
            pool.putTable(hTable);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if (hTable != null) {
                try {
                    hTable.close();
                } catch (IOException e1) {

                }
            }
        }
    }
}
