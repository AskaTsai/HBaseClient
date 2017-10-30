package com.sselab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class HBaseDemo {
    Configuration conf;
    Connection conn;
    Logger logger = Logger.getLogger(String.valueOf(HBaseDemo.class));

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum","tmphmaster,tmpslave,tmpslave2");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void createTable() throws IOException {
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        if (admin.tableExists("people")) {
            System.out.println("表已经存在");
            return;
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people"));
        HColumnDescriptor hcd_info = new HColumnDescriptor("info");
        htd.addFamily(hcd_info);
        htd.addFamily(new HColumnDescriptor("data"));
        hcd_info.setMaxVersions(3);
        admin.createTable(htd);
        admin.close();
        System.out.println("表创建完成！！");
    }
    @Test
    public void testPut() throws IOException {
        HTable table = (HTable) conn.getTable(TableName.valueOf("people"));

        Put put = new Put(Bytes.toBytes("rk0001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name" ), Bytes.toBytes("zhangsan" ));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age" ), Bytes.toBytes("25" ));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money" ), Bytes.toBytes("10w" ));
        table.put(put);
    }
    @Test
    public void testPutAll() throws IOException {
        long start = System.currentTimeMillis();
        HTable table = (HTable) conn.getTable(TableName.valueOf("people"));

        List<Put> puts = new ArrayList<>(10000);
        for (int i = 1; i < 100001; i++) {
            Put put = new Put(Bytes.toBytes("rk" + String.format("%05d",i)));
            put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("money" ), Bytes.toBytes("" + i ));
            puts.add(put);
            if (i % 10000 == 0) {
                table.put(puts);
                puts = new ArrayList<>(10000);
            }
        }
        long end = System.currentTimeMillis();
        logger.info("插入10万数据用时：" + (end - start) + "ms");
    }
    @Test
    public void testGet() throws IOException {
        Table table = (HTable) conn.getTable(TableName.valueOf("people"));
        Get get = new Get(Bytes.toBytes("rk09999"));
        Result result = table.get(get);
        String str = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
        logger.info("Get结果:" + str);
    }

    @Test
    public void testScan() throws IOException {
        Table table = (HTable) conn.getTable(TableName.valueOf("people"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "RowName: " + new String(CellUtil.cloneRow(cell)) + "\t" +
                        "Timestamp: " + cell.getTimestamp() + "\t" +
                        "Column Family: " + new String(CellUtil.cloneFamily(cell)) + "\t" +
                        "ColumnName: " + new String(CellUtil.cloneQualifier(cell))  + "\t" +
                        "Value: " + new String(CellUtil.cloneValue(cell))
                );
            }
        }
    }

    @Test
    public void testDelete() throws IOException {
        Table table = (HTable) conn.getTable(TableName.valueOf("people"));
        Delete delete = new Delete(Bytes.toBytes("rk 9999"));
        //删除指定列族
        delete.addFamily(Bytes.toBytes("info"));
        //删除指定列
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"));
        table.delete(delete);
    }

    @Test
    public void testBatchDelete() throws IOException {
        Table table = (HTable) conn.getTable(TableName.valueOf("people"));
        List<Delete> deleteList = new ArrayList<>();
        for(int i = 1; i < 100001; i++){
            Delete delete = new Delete(Bytes.toBytes("rk" + String.format("%05d", i)));
            deleteList.add(delete);
        }
        table.delete(deleteList);
    }
}
