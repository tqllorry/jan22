package analysis.mr.new_total_user;

import analysis.base.DateDimension;
import analysis.mr.active_user.MR_ActiveDriver;
import analysis.mr.active_user.My_ActiveMap;
import analysis.mr.active_user.My_ActiveReduce;
import analysis.mr.key.StatsUserDimension;
import analysis.mr.value.MapOutPutValue;
import analysis.mr.value.ReduceOutputValue;
import analysis.service.impl.IDimensionImpl;
import common.DateEnum;
import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import util.JdbcUtil;
import util.TimeUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class Total_Driver implements Tool {
    private static Logger logger = Logger.getLogger(Total_Driver.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        //handleArgs(conf, args);
        Job job = Job.getInstance(conf, "total");
        job.setJarByClass(MR_ActiveDriver.class);

        //map端
        job.setMapperClass(My_ActiveMap.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(MapOutPutValue.class);

        //reduce
        job.setReducerClass(My_ActiveReduce.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(ReduceOutputValue.class);

        //路径
        //handlePath(job);
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\tqllorry\\Desktop\\23"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\tqllorry\\Desktop\\output"));

        //return job.waitForCompletion(true) ? 0 : 1;
        if (job.waitForCompletion(true)) {
            this.computTotal(job);
            return 0;
        } else {
            return 1;
        }
    }

    private void computTotal(Job job) {
        String nowDate = job.getConfiguration().get(GlobalConstants.RUNDATE);
        long nowDateMiles = TimeUtil.parseString2Long(nowDate);
        long yesterDateMiles = nowDateMiles - GlobalConstants.MILES_A_DAY;

        DateDimension nowDateDimension = DateDimension.getInstance(nowDateMiles, DateEnum.DAY);
        DateDimension yesterDateDimension = DateDimension.getInstance(yesterDateMiles, DateEnum.DAY);

        IDimensionImpl iDimension = new IDimensionImpl();

        int nowDateID = 0;
        int yesterDateID = 0;

        try {
            nowDateID = iDimension.getDimensionIdByObj(nowDateDimension);
            yesterDateID = iDimension.getDimensionIdByObj(yesterDateDimension);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Connection conn = JdbcUtil.getConn();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();

        String sql1 = "SELECT new_install_user,platform_dimension_id FROM stats_user WHERE date_dimension_id=?";
        String sql2 = "SELECT total_install_user,platform_dimension_id FROM stats_user WHERE date_dimension_id=?";
        String sql3 = "insert into 'stats_user' (`platform_dimension_id`,`date_dimension_id`,`total_install_users`,`created`) values (?,?,?,?)";
        try {
            //昨天的新增总用户
            ps = conn.prepareStatement(sql2);
            ps.setInt(1, yesterDateID);
            rs = ps.executeQuery();

            while (rs.next()) {
                int total_user = rs.getInt(1);
                int platform_id = rs.getInt(2);
                map.put(platform_id, total_user);
            }

            //今天的新增用户，相同的增加，没有的total为新增
            ps = conn.prepareStatement(sql1);
            ps.setInt(1, nowDateID);
            rs = ps.executeQuery();

            while (rs.next()) {
                int new_install_user = rs.getInt(1);
                int platform_id = rs.getInt(2);
                if (map.containsKey(platform_id)) {
                    new_install_user = map.get(platform_id) + new_install_user;
                }
                map.put(platform_id, new_install_user);
            }

            for (Map.Entry<Integer, Integer> m : map.entrySet()) {
                ps = conn.prepareStatement(sql3);
                ps.setInt(1, m.getKey());
                ps.setInt(2, nowDateID);
                ps.setInt(3, m.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNDATE));
                rs = ps.executeQuery();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
    }

    private void handlePath(Job job) {
        //设置路径
        String[] str = job.getConfiguration().get(GlobalConstants.RUNDATE).split("-");
        String m = str[1];
        String d = str[2];
        try {
            Path p1 = new Path("/logs/" + m + "/" + d);
            Path p2 = new Path("/ods/" + m + "/" + d);
            FileSystem fs = FileSystem.get(conf);

            //设置输入路径
            if (fs.exists(p1)) {
                FileInputFormat.addInputPath(job, p1);
            } else {
                logger.error("输入的路径 " + p1 + "不存在!");
            }
            //设置输出路径
            if (fs.exists(p2)) {
                fs.delete(p2, true);
            }
            FileOutputFormat.setOutputPath(job, p2);
        } catch (IOException e) {
            logger.error("设置输入输出路径异常", e);
        }
    }

    //给job添加日期属性
    private void handleArgs(Configuration conf, String[] args) {
        //by 2019-01-22
        String date = null;

        //判断日期格式
        String reg = "\\d{4}-\\d{2}-\\d{2}";
        Pattern pattern = Pattern.compile(reg);

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("by")) {
                if (i + 1 < args.length) {
                    if (pattern.matcher(args[i + 1]).matches()) {
                        date = args[i + 1];
                    }
                }
            }
        }

        //没有参数就默认为前一天日期
        if (StringUtils.isEmpty(date)) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DAY_OF_YEAR, -1);
            date = simpleDateFormat.format(calendar.getTime());
        }

        conf.set(GlobalConstants.RUNDATE, date);
    }

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("core-site.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {

        try {
            ToolRunner.run(new Configuration(), new Total_Driver(), args);
        } catch (Exception e) {
            logger.error("执行异常", e);
        }
    }
}
