package analysis.service.impl;

import analysis.base.*;
import analysis.service.IDimension;
import util.JdbcUtil;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Description: Jdbc<br/>
 * Copyright(c), 2018, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2018年12月20日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class IDimensionImpl implements IDimension {
    //定义内存缓存，用来缓存维度id
    //缓存中value是ID
    //缓存中的key怎么存？
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    @Override
    public int getDimensionIdByObj(BaseDimension dimension) throws IOException {
        //获取数据库连接
        Connection conn = null;
        //构建缓存的key，然后使用这个key去缓存中查看
        String cacheKey = buildCacheKey(dimension);
        //获取到key之后，先去缓存中去查
        if (cache.containsKey(cacheKey)) {
            return this.cache.get(cacheKey);
        }

        //代码走到这里说明缓存中不存在该对象对应的维度ID
        //我们就要到数据库中去查
        String[] sqls = null;
        if (dimension instanceof KpiDimension) {
            sqls = buildKpiSqls(dimension);
        } else if (dimension instanceof BrowserDimension) {
            sqls = buildBrowserSqls(dimension);
        } else if (dimension instanceof DateDimension) {
            sqls = buildDateSqls(dimension);
        } else if (dimension instanceof PlatformDimension) {
            sqls = buildPlatSqls(dimension);
        }

        //获取连接
        conn = JdbcUtil.getConn();
        int id = -1;
        synchronized (this) {
            id = this.excutSql(conn, sqls, dimension);
        }
        return id;
    }

    private int excutSql(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String selectSql = sqls[0];
            //获取ps对象
            ps = conn.prepareStatement(selectSql);
            //设置参数，也就是为ps赋值
            this.setArgs(dimension, ps);//为插入语句赋值
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            //代码走到这里，说明没有查询出来
            //那么就要执行插入语句，并且返回维度ID
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            //为插入语句赋值
            this.setArgs(dimension, ps);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null, ps, rs);
        }
        return -1;
    }

    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if (dimension instanceof KpiDimension) {
                KpiDimension kpiDimension = (KpiDimension) dimension;
                ps.setString(++i, kpiDimension.getKpiName());
            } else if (dimension instanceof BrowserDimension) {
                BrowserDimension browserDimension = (BrowserDimension) dimension;
                ps.setString(++i, browserDimension.getBrowserName());
                ps.setString(++i, browserDimension.getBrowserVersion());
            } else if (dimension instanceof DateDimension) {
                DateDimension dateDimension = (DateDimension) dimension;
                ps.setInt(++i, dateDimension.getYear());
                ps.setInt(++i, dateDimension.getSeason());
                ps.setInt(++i, dateDimension.getMonth());
                ps.setInt(++i, dateDimension.getWeek());
                ps.setInt(++i, dateDimension.getDay());
                ps.setString(++i, dateDimension.getType());
                ps.setDate(++i, new Date(dateDimension.getCalendar().getTime()));
            } else if (dimension instanceof PlatformDimension) {
                PlatformDimension platformDimension = (PlatformDimension) dimension;
                ps.setString(++i, platformDimension.getPlatformName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String[] buildBrowserSqls(BaseDimension dimension) {
        String select = "select id from `dimension_browser` where `browser_name` = ? and `browser_version` = ?";
        String insert = "insert into `dimension_browser` (`browser_name`,`browser_version`)values(?,?)";
        return new String[]{select, insert};
    }

    private String[] buildKpiSqls(BaseDimension dimension) {
        String select = "select id from `dimension_kpi` where kpi_name = ?";
        String insert = "insert into `dimension_kpi` (kei_name) values(?)";
        return new String[]{select, insert};
    }

    private String[] buildPlatSqls(BaseDimension dimension) {
        String insertSql = "insert into `dimension_platform`(platform_name) values(?)";
        String selectSql = "select id from `dimension_platform` where platform_name = ?";
        return new String[]{selectSql, insertSql};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        String selectSql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
        return new String[]{selectSql, insertSql};
    }

    //除了id之外的其他字段
    private String buildCacheKey(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof KpiDimension) {
            sb.append("kpi_");
            KpiDimension kpiDimension = (KpiDimension) baseDimension;
            sb.append(kpiDimension.getKpiName());
        } else if (baseDimension instanceof BrowserDimension) {
            sb.append("browser_");
            BrowserDimension browserDimension = (BrowserDimension) baseDimension;
            sb.append(((BrowserDimension) baseDimension).getBrowserName());
            sb.append(browserDimension.getBrowserVersion());
        } else if (baseDimension instanceof DateDimension) {
            sb.append("date_");
            DateDimension dateDimension = (DateDimension) baseDimension;
            sb.append(dateDimension.getYear());
            sb.append(dateDimension.getSeason());
            sb.append(dateDimension.getMonth());
            sb.append(dateDimension.getWeek());
            sb.append(dateDimension.getDay());
            sb.append(dateDimension.getType());
        } else if (baseDimension instanceof PlatformDimension) {
            sb.append("platform_");
            PlatformDimension platformDimension = (PlatformDimension) baseDimension;
            sb.append(platformDimension.getPlatformName());
        }
        return sb != null ? sb.toString() : null;
    }
}
