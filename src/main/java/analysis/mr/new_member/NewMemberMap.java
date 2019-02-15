package analysis.mr.new_member;

import analysis.base.BrowserDimension;
import analysis.base.DateDimension;
import analysis.base.KpiDimension;
import analysis.base.PlatformDimension;
import analysis.mr.key.StatsCommonDimension;
import analysis.mr.key.StatsUserDimension;
import analysis.mr.value.MapOutPutValue;
import common.DateEnum;
import common.GlobalConstants;
import common.KpiEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import util.MemberUtil;

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
public class NewMemberMap extends Mapper<LongWritable, Text, StatsUserDimension, MapOutPutValue> {
    private static Logger logger = Logger.getLogger(NewMemberMap.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private MapOutPutValue mapOutPutValue = new MapOutPutValue();

    //两个kpi维度
    private KpiDimension newMemberKpi = new KpiDimension(KpiEnum.NEW_MEMBER.name);
    private KpiDimension newMemberBrowserKpi = new KpiDimension(KpiEnum.NEW_BROWSER_MEMBER.name);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //删除统计当天新增会员
        MemberUtil.deleteByDay(context.getConfiguration().get(GlobalConstants.RUNDATE));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //过滤空数据
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }

        //11541924731411e_pv368E2709-CB14-4D1D-9D22-84182C2689CCnullE6071242-D8E8-49B8-904D-492EDE34D13A1535610938390zh-CNMozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.361600*900http://localhost:8080/demo2.jspnull测试页面2website61.159.151.121nullnullnullnullnullnullnullnullnullChrome31.0.1650.63WindowsWindows 7中国贵州省黔西南州兴义市
        String[] sp = line.split("\\u0001");
        String s_time = sp[1];
        String pl = sp[13];
        String u_mid = sp[4];
        String browserName = sp[24];
        String browserVersion = sp[25];

        //过滤无效数据
        if (StringUtils.isEmpty(s_time) || StringUtils.isEmpty(u_mid) || u_mid.equals("null")) {
            logger.info("时间戳或者memberId不能为空");
            return;
        }

        if (!MemberUtil.isNewMember1(u_mid, context.getConfiguration())) {
            logger.info("不是新会员");
            return;
        }

        DateDimension dateDimension = DateDimension.getInstance(Long.valueOf(s_time), DateEnum.DAY);
        PlatformDimension platformDimension = PlatformDimension.getInstance(pl);
        BrowserDimension nullBrowserDimension = new BrowserDimension();

        StatsCommonDimension statsCommonDimension = new StatsCommonDimension(dateDimension, platformDimension, newMemberKpi);

        statsUserDimension.setBrowserDimension(nullBrowserDimension);
        statsUserDimension.setStatsCommonDimension(statsCommonDimension);

        mapOutPutValue.setId(u_mid);
        mapOutPutValue.setTime(Long.valueOf(s_time));

        //新增会员
        context.write(statsUserDimension, mapOutPutValue);

        BrowserDimension browserDimension = new BrowserDimension(browserName, browserVersion);
        statsUserDimension.setBrowserDimension(browserDimension);
        statsCommonDimension.setKpi(newMemberBrowserKpi);
        statsUserDimension.setStatsCommonDimension(statsCommonDimension);

        //新增浏览器会员
        context.write(statsUserDimension, mapOutPutValue);
    }
}
