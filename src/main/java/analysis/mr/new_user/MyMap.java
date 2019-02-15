package analysis.mr.new_user;

import analysis.base.BrowserDimension;
import analysis.base.DateDimension;
import analysis.base.KpiDimension;
import analysis.base.PlatformDimension;
import analysis.mr.key.StatsCommonDimension;
import analysis.mr.key.StatsUserDimension;
import analysis.mr.value.MapOutPutValue;
import common.DateEnum;
import common.EventEnum;
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
public class MyMap extends Mapper<LongWritable, Text, StatsUserDimension, MapOutPutValue> {
    private static Logger logger = Logger.getLogger(MyMap.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private MapOutPutValue mapOutPutValue = new MapOutPutValue();

    //两个kpi维度
    private KpiDimension newUserKpi = new KpiDimension(KpiEnum.NEW_USER.name);
    private KpiDimension newBrowserKpi = new KpiDimension(KpiEnum.BROWSER_NEW_USER.name);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //过滤空数据
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }

        //11541924731411e_pv368E2709-CB14-4D1D-9D22-84182C2689CCnullE6071242-D8E8-49B8-904D-492EDE34D13A1535610938390zh-CNMozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.361600*900http://localhost:8080/demo2.jspnull测试页面2website61.159.151.121nullnullnullnullnullnullnullnullnullChrome31.0.1650.63WindowsWindows 7中国贵州省黔西南州兴义市
        String[] sp = line.split("\\u0001");
        String en = sp[2];

        //过滤新用户
        if (StringUtils.isNotEmpty(en) && en.equals(EventEnum.LANUCH.alias)) {
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

            StatsCommonDimension statsCommonDimension = new StatsCommonDimension(dateDimension, platformDimension, newUserKpi);

            statsUserDimension.setBrowserDimension(nullBrowserDimension);
            statsUserDimension.setStatsCommonDimension(statsCommonDimension);

            mapOutPutValue.setId(u_ud);
            //mapOutPutValue.setTime(Long.valueOf(s_time));

            //新增用户
            context.write(statsUserDimension, mapOutPutValue);

            BrowserDimension browserDimension = new BrowserDimension(browserName, browserVersion);
            statsUserDimension.setBrowserDimension(browserDimension);
            statsCommonDimension.setKpi(newBrowserKpi);
            statsUserDimension.setStatsCommonDimension(statsCommonDimension);

            //新增浏览器用户
            context.write(statsUserDimension, mapOutPutValue);
        }
    }
}