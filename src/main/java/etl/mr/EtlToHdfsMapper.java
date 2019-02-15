package etl.mr;

import etl.mr.LogWritable;
import etl.util.LogUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月22日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class EtlToHdfsMapper extends Mapper<LongWritable, Text, LogWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LogWritable k = LogUtil.parseLog(value.toString());
        System.out.println(k);
        context.write(k, NullWritable.get());
    }
}
