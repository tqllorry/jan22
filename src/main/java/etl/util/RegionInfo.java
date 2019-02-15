package etl.util;

import common.GlobalConstants;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月22日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class RegionInfo {
    private String DEFAULT_VALUE = GlobalConstants.DEFAULT_VALUE;
    private String country = DEFAULT_VALUE;
    private String province = DEFAULT_VALUE;
    private String city = DEFAULT_VALUE;

    public RegionInfo(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public RegionInfo() {
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "RegionInfo{" +
                "country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
