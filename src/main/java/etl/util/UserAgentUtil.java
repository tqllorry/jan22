package etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
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
public class UserAgentUtil {
    private static Logger logger = Logger.getLogger(UserAgentUtil.class);
    private static UASparser uaSparser = null;

    //静态代码块
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("获取uaParser对象异常");
        }
    }

    public static BroswerInfo broswerParse(String biev) {
        BroswerInfo broswerInfo = new BroswerInfo();
        try {
            UserAgentInfo parse = UserAgentUtil.uaSparser.parse(biev);
            broswerInfo.setBrowserName(parse.getUaFamily());
            broswerInfo.setBrowserVersion(parse.getBrowserVersionInfo());
            broswerInfo.setOsName(parse.getOsFamily());
            broswerInfo.setOsVersion(parse.getOsName());
            //System.out.println(broswerInfo);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return broswerInfo;
    }

    public static void main(String[] args) {
        //System.out.println(broswerParse("Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F71.0.3578.98%20Safari%2F537.36")) ;
    }
}
