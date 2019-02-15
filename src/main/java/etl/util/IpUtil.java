package etl.util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月22日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class IpUtil {
    private static Logger logger = Logger.getLogger(IpUtil.class);
    public static RegionInfo regionInfo = new RegionInfo();

    /**
     * 纯真数据库解析IP地址
     *
     * @param ip
     * @return
     */
    public static RegionInfo ipParser(String ip) {
        if (StringUtils.isNotEmpty(ip)) {
            String country = IPSeeker.getInstance().getCountry(ip);
            if (StringUtils.isNotEmpty(country.trim())) {
                if (country.equals("局域网")) {
                    //System.out.println(country);
                    regionInfo.setCountry("中国");
                    regionInfo.setProvince("北京市");
                    regionInfo.setCity("昌平区");
                } else {
                    int index = country.indexOf("省");
                    if (index > 0) {
                        regionInfo.setCountry("中国");
                        regionInfo.setProvince(country.substring(0, index + 1));
                        int index2 = country.indexOf("市");
                        if (index2 > 0) {
                            regionInfo.setCity(country.substring(index + 1, index2 + 1));
                        }
                    } else {
                        String flag = country.substring(0, 2);
                        switch (flag) {
                            case "内蒙":
                                regionInfo.setProvince("内蒙古");
                                country.substring(3);
                                index = country.indexOf("市");
                                if (index > 0) {
                                    regionInfo.setCity(country.substring(0, index + 1));
                                }
                                break;
                            case "宁夏":
                            case "广西":
                            case "新疆":
                            case "西藏":
                                regionInfo.setProvince(flag + "省");
                                country.substring(2);
                                index = country.indexOf("市");
                                if (index > 0) {
                                    regionInfo.setCity(country.substring(0, index + 1));
                                }
                                break;
                            case "北京":
                            case "天津":
                            case "上海":
                            case "重庆":
                                regionInfo.setProvince(flag + "市");
                                country.substring(2);
                                index = country.indexOf("区");
                                if (index > 0) {
                                    char ch = country.charAt(index - 1);
                                    if (ch != '小' || ch != '校' || ch != '军') {
                                        regionInfo.setCity(country.substring(0, index + 1));
                                    }
                                }

                                index = country.indexOf("县");
                                if (index > 0) {
                                    regionInfo.setCity(country.substring(0, index + 1));
                                }
                                break;
                            case "香港":
                            case "澳门":
                            case "台湾":
                                regionInfo.setProvince(flag + "特别行政区");
                                break;
                            default:
                                break;

                        }
                    }
                }
            }
        }
        return regionInfo;
    }

    public static void main(String[] args) {
        System.out.println(ipParser("61.159.151.122"));
    }
}
