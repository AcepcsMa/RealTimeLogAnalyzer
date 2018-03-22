package dao;

import domain.CategoryClickCount;
import util.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class of click count DAO.
 */
public class CategoryClickCountDAO {

    public static final String TABLE_STAT_BY_TIME = "category_clickcount";
    public static final String TABLE_STAT_BY_TIME_REFERRER = "category_referrer_clickcount";

    /**
     * Query click count stat by time / by time & referrer.
     * @param time time
     * @param referrer referrer
     * @return a list of click count records
     * @throws IOException
     */
    public List<CategoryClickCount> queryByTime(String time, String referrer) throws IOException {

        String prefixFilter = null;
        String tableName = null;
        if (referrer != null) {
            tableName = TABLE_STAT_BY_TIME_REFERRER;
            prefixFilter = time + "_" + referrer; // query by both time & referrer
        } else {
            tableName = TABLE_STAT_BY_TIME;
            prefixFilter = time; // query only by time
        }
        List<CategoryClickCount> result = new ArrayList<>();
        Map<String, Long> records = HBaseUtils.getInstance().queryByTime(tableName, prefixFilter);
        for(Map.Entry<String, Long> record : records.entrySet()) {
            CategoryClickCount clickCount = new CategoryClickCount();
            clickCount.setCategoryName(record.getKey());
            clickCount.setValue(record.getValue());
            result.add(clickCount);
        }
        return result;
    }
}
