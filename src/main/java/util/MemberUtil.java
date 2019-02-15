package util;

import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Description: 会员工具类<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年02月12日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class MemberUtil {
    protected static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 5000;
        }
    };

    private static Jedis jedis = null;

    static {
        jedis = RedisUtil.getJedis();
    }

    /**
     * @param id
     * @return
     */
    public static boolean isNewMember1(String id, Configuration conf) {
        Connection conn = JdbcUtil.getConn();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql1 = "select `member_id` from `member_info` where `member_id` = ?";
        String sql2 = "insert into `member_info` (`member_id`,`member_id_server_date`) values (?,?)";

        if (cache.containsKey(id)) {
            return cache.get(id);
        }

        try {
            ps = conn.prepareStatement(sql1);
            ps.setString(1, id);
            rs = ps.executeQuery();

            if (rs.next()) {
                cache.put(id, false);
                return false;
            } else {
                ps = conn.prepareStatement(sql2);
                ps.setString(1, id);
                ps.setString(2, conf.get(GlobalConstants.RUNDATE));
                cache.put(id, false);
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean isNewMember2(String id, String date) {
        if (StringUtils.isEmpty(id)) {
            return false;
        }

        Boolean b = cache.get(id);
        if (b != null) {
            return false;
        }

        jedis.select(1);
        Set<String> keys = jedis.keys("*_" + id);

        if (keys.size() > 0) {
            cache.put(id, false);
            return false;
        } else {
            jedis.set(date + "_" + id, id);
            cache.put(id, false);
            return true;
        }
    }

    public static void deleteByDay(String date) {
        Connection conn = JdbcUtil.getConn();
        PreparedStatement ps = null;
        try {
            String sql = "delete from member_info where created = ?";
            ps = conn.prepareStatement(sql);
            ps.setString(1, date);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null, ps, null);
        }
    }
}
