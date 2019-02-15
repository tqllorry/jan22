package analysis.base;

import common.DateEnum;
import util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class DateDimension extends BaseDimension {
    private int id; // id，eg: 1
    private int year; // 年份: eg: 2015
    private int season; // 季度，eg:4
    private int month; // 月份,eg:12
    private int week; // 周
    private int day;
    private String type; // 类型
    private Date calendar = new Date();

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public DateDimension() {
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, String type, Date calendar) {
        this.id = id;
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
        this.calendar = calendar;
    }

    public DateDimension(int year, int season, int month, int week, int day, String type, Date calendar) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
        this.calendar = calendar;
    }

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension d = (DateDimension) o;
        if (this == o) {
            return 0;
        } else if (this.id != d.id) {
            return this.id - d.id;
        } else if (this.year != d.year) {
            return this.year - d.year;
        } else if (this.season != d.season) {
            return this.season - d.season;
        } else if (this.month != d.month) {
            return this.month - d.month;
        } else if (this.week != d.week) {
            return this.week - d.year;
        } else if (this.day != d.day) {
            return this.day - d.day;
        } else {
            return this.type.compareTo(d.type);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.year);
        out.writeInt(this.season);
        out.writeInt(this.month);
        out.writeInt(this.week);
        out.writeInt(this.day);
        out.writeUTF(this.type);
        out.writeLong(this.calendar.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readInt();
        this.season = in.readInt();
        this.month = in.readInt();
        this.week = in.readInt();
        this.day = in.readInt();
        this.type = in.readUTF();
        this.calendar.setTime(in.readLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(type, that.type) &&
                Objects.equals(calendar, that.calendar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, year, season, month, week, day, type, calendar);
    }

    public static DateDimension getInstance(long time, DateEnum type) {
        Calendar c = Calendar.getInstance();
        c.clear();

        int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);
        if (type.equals(DateEnum.YEAR)) {
            c.setTimeInMillis(time);
            return new DateDimension(year, 0, 0, 0, 1, type.dateType, c.getTime());
        }

        int season = TimeUtil.getDateInfo(time, DateEnum.SEASON);
        if (type.equals(DateEnum.SEASON)) {
            int month = season * 3 - 2;
            c.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 1, type.dateType, c.getTime());
        }

        int month = TimeUtil.getDateInfo(time, DateEnum.MONTH);
        if (type.equals(DateEnum.MONTH)) {
            c.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 1, type.dateType, c.getTime());
        }

        int week = TimeUtil.getDateInfo(time, DateEnum.WEEK);
        if (type.equals(DateEnum.WEEK)) {
            long f = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(f, DateEnum.YEAR);
            month = TimeUtil.getDateInfo(f, DateEnum.MONTH);
            int day = TimeUtil.getDateInfo(f, DateEnum.DAY);
            season = TimeUtil.getDateInfo(f, DateEnum.SEASON);
            c.set(year, month - 1, day);
            return new DateDimension(year, season, month, week, day, type.dateType, c.getTime());
        }

        if (type.equals(DateEnum.DAY)) {
            int day = TimeUtil.getDateInfo(time, DateEnum.DAY);
            return new DateDimension(year, season, month, week, day, type.dateType, c.getTime());
        }
        throw new RuntimeException("该日期类型支持获取时间维度对象，datetype" + type.dateType);
    }

    @Override
    public String toString() {
        return "DateDimension{" +
                "id=" + id +
                ", year=" + year +
                ", season=" + season +
                ", month=" + month +
                ", week=" + week +
                ", day=" + day +
                ", type='" + type + '\'' +
                ", calendar=" + calendar +
                '}';
    }
}
