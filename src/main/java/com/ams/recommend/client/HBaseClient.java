package com.ams.recommend.client;

import com.ams.recommend.util.Property;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseClient {

    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"));
        conf.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"));
        conf.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"));
        conf.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"));
        conf.set("hbase.client.ipc.pool.size", Property.getStrValue("hbase.client.ipc.pool.size"));
    }

    /**
     * 查看表是否存在
     * @param tableName
     * @return
     */
    public static boolean existTable(String tableName) {
        boolean exist = false;
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Admin admin = conn.getAdmin();
            exist = admin.tableExists(TableName.valueOf(tableName));
            admin.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        return exist;
    }

    /**
     * 创建一个新的表
     * @param tableName
     */
    public static void createTableIfNotExist(String tableName, String... family) {
        if(!existTable(tableName)) createOrOverwriteTable(tableName, family);
        else {
            logger.error("Table : " + tableName + " already existed");
            return;
        }
    }

    /**
     * 创建或覆盖表
     * @param tableName 表名
     */
    public static void createOrOverwriteTable(String tableName, String... cfs) {
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Admin admin = conn.getAdmin();
            TableName tName = TableName.valueOf(tableName);

            if(admin.tableExists(tName)) {
                admin.enableTable(tName);   //先停掉table
                admin.disableTable(tName);  //后删除table
            }

            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            for (String cf : cfs) {
                columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(cf))
                        .build());
            }

            TableDescriptor tableDescriptor = TableDescriptorBuilder
                    .newBuilder(tName)
                    .setColumnFamilies(columnFamilyDescriptors) //设置添加列族
                    .build();
            admin.createTable(tableDescriptor);  //创建table

            admin.close();  //及时关闭流
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入一列数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     */
    public static void put(String tableName, String rowKey, String family, String column, String value) {
        TableName tName = TableName.valueOf(tableName);
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(tName);
            Put put = new Put(Bytes.toBytes(rowKey))
                    .addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);

            table.close();  //及时关闭流
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addOrUpdateColumn(String tableName, String rowKey, String family, String column) {
        String count = get(tableName, rowKey, family, column);
        if(count == null) count = "0";

        put(tableName, rowKey, family, column, String.valueOf(Long.valueOf(count) + 1));
    }

    public static String get(String tableName, String rowKey, String family, String column) {
        String res = null;
        TableName tName = TableName.valueOf(tableName);
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(tName);
            Get get = new Get(Bytes.toBytes(rowKey))
                    .addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            Result rs = table.get(get);
            res = Bytes.toString(rs.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 获取一整行
     * @return
     */
    public static Map<String, String> getRow(String tableName, String rowKey) {
        Map<String, String> kv = new HashMap<>();
        TableName tName = TableName.valueOf(tableName);
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(tName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result rs = table.get(get);
            for (Cell cell : rs.listCells()){
                String key = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                kv.put(key, value);
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kv;
    }

    public static int getColumnSize(String tableName, String rowKey, String family) {
        int size = 0;
        TableName tName = TableName.valueOf(tableName);
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(tName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result rs = table.get(get);

            if(rs.isEmpty()) return 0;

            Map<byte[], byte[]> familyMap = rs.getFamilyMap(Bytes.toBytes(family));
            size = familyMap.keySet().size();
            table.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }

    public static void createRow(String tableName, String rowKey, String c, String count, String value) {
        TableName tName = TableName.valueOf(tableName);
        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(tName);
            Put put = new Put(Bytes.toBytes(rowKey))
                    .addColumn(Bytes.toBytes(c), Bytes.toBytes(count), Bytes.toBytes(value));
            table.put(put);
            table.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
