package util;

import common.DateEnum;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class TimeUtil {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    /**
     * 获取日期信息
     *
     * @param time
     * @param type
     * @return
     */
    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (type.equals(DateEnum.YEAR)) {
            return calendar.get(Calendar.YEAR);
        }
        if (type.equals(DateEnum.SEASON)) {
            int month = calendar.get(Calendar.MONTH) + 1;
            return month % 3 == 0 ? month / 3 : (month / 3 + 1);
        }
        if (type.equals(DateEnum.MONTH)) {
            return calendar.get(Calendar.MONTH) + 1;
        }
        if (type.equals(DateEnum.WEEK)) {
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }
        if (type.equals(DateEnum.DAY)) {
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if (type.equals(DateEnum.HOUR)) {
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw new RuntimeException("不支持该类型的日期信息获取.type：" + type.dateType);
    }

    /**
     * 将默认的日期格式转换成时间戳
     *
     * @param date
     * @return
     */
    public static long parseString2Long(String date) {
        return parseString2Long(date, DEFAULT_FORMAT);
    }

    public static long parseString2Long(String date, String pattern) {
        Date dt = null;

        try {
            dt = new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dt.getTime();
    }

    /**
     * 获取某周第一天时间戳
     *
     * @param time
     * @return
     */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        //
        calendar.set(Calendar.DAY_OF_WEEK, 1);//该周的第一天
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
}
