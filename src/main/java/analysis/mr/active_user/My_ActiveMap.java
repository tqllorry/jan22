package analysis.mr.active_user;

import analysis.base.BrowserDimension;
import analysis.base.DateDimension;
import analysis.base.KpiDimension;
import analysis.base.PlatformDimension;
import analysis.mr.key.StatsCommonDimension;
import analysis.mr.key.StatsUserDimension;
import analysis.mr.value.MapOutPutValue;
import common.DateEnum;
import common.KpiEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class My_ActiveMap extends Mapper<LongWritable, Text, StatsUserDimension, MapOutPutValue> {
    private static Logger logger = Logger.getLogger(My_ActiveMap.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private MapOutPutValue mapOutPutValue = new MapOutPutValue();

    //两个kpi维度
    private KpiDimension ActiveUserKpi = new KpiDimension(KpiEnum.ACTIVE_USER.name);
    private KpiDimension ActiveBrowserKpi = new KpiDimension(KpiEnum.BROWSER_ACTIVE_USER.name);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //过滤空数据
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }

        String[] sp = line.split("\\u0001");
        String en = sp[2];

        //过滤新用户
        if (StringUtils.isNotEmpty(en)) {
            String s_time = sp[1];
            String pl = sp[13];
            String u_ud = sp[3];
            String browserName = sp[24];
            String browserVersion = sp[25];

            //过滤无效数据
            if (StringUtils.isEmpty(s_time) || StringUtils.isEmpty(u_ud) || u_ud.equals("null")) {
                logger.info("时间戳或者uuid不能为空");
                return;
            }

            System.out.println(s_time);
            DateDimension dateDimension = DateDimension.getInstance(Long.valueOf(s_time), DateEnum.MONTH);
            System.out.println(dateDimension);
            PlatformDimension platformDimension = PlatformDimension.getInstance(pl);
            BrowserDimension nullBrowserDimension = new BrowserDimension();

            StatsCommonDimension statsCommonDimension = new StatsCommonDimension(dateDimension, platformDimension, ActiveUserKpi);

            statsUserDimension.setBrowserDimension(nullBrowserDimension);
            statsUserDimension.setStatsCommonDimension(statsCommonDimension);

            mapOutPutValue.setId(u_ud);
            mapOutPutValue.setTime(Long.valueOf(s_time));

            context.write(statsUserDimension, mapOutPutValue);

            BrowserDimension browserDimension = new BrowserDimension(browserName, browserVersion);
            statsUserDimension.setBrowserDimension(browserDimension);
            statsCommonDimension.setKpi(ActiveBrowserKpi);
            statsUserDimension.setStatsCommonDimension(statsCommonDimension);

            context.write(statsUserDimension, mapOutPutValue);
        }
    }
}