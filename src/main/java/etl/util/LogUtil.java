package etl.util;

import etl.mr.LogWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月22日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class LogUtil {
    private static Logger logger = Logger.getLogger(LogUtil.class);
    //private static LogWritable logWritable = null;

    public static LogWritable parseLog(String log) {
        LogWritable logWritable = new LogWritable();
        //61.159.151.121^A1541924731.005^A61.159.151.121^A/qf.png?en=e_l&ver=1&pl=website&sdk=js&u_ud=07E02E08-21A2-48A5-9C03-3F234EAB0270&u_sd=3723795D-D53D-4117-B529-824E31B89E77&c_time=1535610846000&l=zh-CN
        // &b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F58.0.3029.110%20Safari%2F537.36%20SE%202.X%20MetaSr%201.0&b_rst=1600*900
        String[] arr = log.split("\\^A");
        if (arr.length == 4) {
            String ip = arr[0];
            String time = arr[1].replaceAll("\\.", "");
            String param = arr[3];

            logWritable.setIp(ip);

            //解析ip
            RegionInfo regionInfo = IpUtil.ipParser(ip);
            logWritable.setCountry(regionInfo.getCountry());
            logWritable.setCity(regionInfo.getCity());
            logWritable.setProvince(regionInfo.getProvince());

            //时间戳
            logWritable.setS_time(time);

            //参数列表
            paramToBean(param, logWritable);
        }
        return logWritable;
    }

    private static void paramToBean(String param, LogWritable logWritable) {
        if (StringUtils.isNotEmpty(param)) {
            int index = param.indexOf("?");
            if (index > 0) {
                String[] fields = param.substring(index + 1).split("&");
                for (String field : fields) {
                    String[] kvs = field.split("=");
                    String k = kvs[0];
                    String v = null;
                    try {
                        v = URLDecoder.decode(kvs[1], "utf-8");
                    } catch (UnsupportedEncodingException e) {
                        logger.error("url解码异常", e);
                    }
                    if (k.equals("b_iev")) {
                        BroswerInfo broswerInfo = UserAgentUtil.broswerParse(v);
                        logWritable.setBrowserName(broswerInfo.getBrowserName());
                        logWritable.setBrowserVersion(broswerInfo.getBrowserVersion());
                        logWritable.setOsName(broswerInfo.getOsName());
                        logWritable.setOsVersion(broswerInfo.getOsVersion());
                    }
                    if (StringUtils.isNotEmpty(k)) {
                        switch (k) {
                            case "ver":
                                logWritable.setVer(v);
                                break;
                            case "en":
                                logWritable.setEn(v);
                                break;
                            case "u_ud":
                                logWritable.setU_ud(v);
                                break;
                            case "u_mid":
                                logWritable.setU_mid(v);
                                break;
                            case "u_sd":
                                logWritable.setU_sd(v);
                                break;
                            case "c_time":
                                logWritable.setC_time(v);
                                break;
                            case "l":
                                logWritable.setL(v);
                                break;
                            case "b_iev":
                                logWritable.setB_iev(v);
                                break;
                            case "b_rst":
                                logWritable.setB_rst(v);
                                break;
                            case "p_url":
                                logWritable.setP_url(v);
                                break;
                            case "p_ref":
                                logWritable.setP_ref(v);
                                break;
                            case "tt":
                                logWritable.setTt(v);
                                break;
                            case "pl":
                                logWritable.setPl(v);
                                break;
                            case "oid":
                                logWritable.setOid(v);
                                break;
                            case "on":
                                logWritable.setOn(v);
                                break;
                            case "cua":
                                logWritable.setCua(v);
                                break;
                            case "cut":
                                logWritable.setCut(v);
                                break;
                            case "pt":
                                logWritable.setPt(v);
                                break;
                            case "ca":
                                logWritable.setCa(v);
                                break;
                            case "ac":
                                logWritable.setAc(v);
                                break;
                            case "kv_":
                                logWritable.setKv_(v);
                                break;
                            case "du":
                                logWritable.setDu(v);
                                break;
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        LogWritable logWritable = LogUtil.parseLog("61.159.151.122^A1541955731.008^A61.159.151.121^A/qf.png?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2Fdemo.jsp&p_ref=http%3A%2F%2Flocalhost%3A8080%2Fdemo.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A21&ver=1&pl=website&sdk=js&u_ud=07E02E08-21A2-48A5-9C03-3F234EAB0270&u_sd=3723795D-D53D-4117-B529-824E31B89E77&c_time=1535610846005&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F58.0.3029.110%20Safari%2F537.36%20SE%202.X%20MetaSr%201.0&b_rst=1600*900");
        System.out.println(logWritable);
    }
}
