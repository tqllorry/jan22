package analysis.mr.active_user;

import analysis.mr.key.StatsUserDimension;
import analysis.mr.value.MapOutPutValue;
import analysis.mr.value.ReduceOutputValue;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
public class MR_ActiveDriver implements Tool {
    private static Logger logger = Logger.getLogger(MR_ActiveDriver.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        //handleArgs(conf, args);
        Job job = Job.getInstance(conf, "point1");
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
        return job.waitForCompletion(true) ? 0 : 1;
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
            ToolRunner.run(new Configuration(), new MR_ActiveDriver(), args);
        } catch (Exception e) {
            logger.error("执行异常", e);
        }
    }
}
