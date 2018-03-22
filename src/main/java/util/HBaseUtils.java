package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseUtils {
    private Configuration conf = null;

    private static HBaseUtils INSTANCE = null;
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final String HBASE_ROOT_DIR = "file:///Users/marco/Code/hbase-1.2.6";

    private HBaseUtils() {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", ZK_ADDRESS);
        conf.set("habse.rootdir", HBASE_ROOT_DIR);
    }

    /**
     * Singleton.
     * @return hbase util instance
     */
    public static HBaseUtils getInstance() {
        if(INSTANCE == null) {
            synchronized(HBaseUtils.class) {
                if(INSTANCE == null) {
                    INSTANCE = new HBaseUtils();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Get a hbase table.
     * @param tableName table name
     * @return htable
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * insert data to a hbase table
     * @param tableName table name
     * @param rowKey row key
     * @param cf column family
     * @param qualifier qualifier
     * @param value cell value
     */
    public void put(String tableName, String rowKey, String cf, String qualifier, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(rowKey.getBytes());
        put.add(cf.getBytes(), qualifier.getBytes(), value.getBytes());
        try {
            table.put(put);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Query by time prefix.
     * @param tableName table name
     * @param time time
     * @return a map of data <row key, value>
     * @throws IOException
     */
    public Map<String, Long> queryByTime(String tableName, String time) throws IOException {

        Map<String, Long> result = new HashMap<>();
        HTable table = getTable(tableName);

        String cf = "info";
        String qualifier = "click_count";

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(time.getBytes()));
        ResultScanner results = table.getScanner(scan);
        for(Result record : results) {
            String rowKey = Bytes.toString(record.getRow());
            Long clickCount = Bytes.toLong(record.getValue(cf.getBytes(), qualifier.getBytes()));
            result.put(rowKey, clickCount);
        }
        return result;
    }

    /**
     * Update a value with increment.
     * @param tableName table name
     * @param rowKey row key
     * @param cf column family
     * @param qualifier qualifier
     * @param increment increment
     */
    public void increase(String tableName, String rowKey, String cf, String qualifier, int increment) {
        HTable table = getTable(tableName);
        try {
            table.incrementColumnValue(rowKey.getBytes(), cf.getBytes(), qualifier.getBytes(), increment);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
