package analysis.mr.key;

import analysis.base.BaseDimension;
import analysis.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
public class StatsUserDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension() {
    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsUserDimension s = (StatsUserDimension) o;
        int tmp = this.statsCommonDimension.compareTo(s.statsCommonDimension);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.browserDimension.compareTo(s.browserDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.browserDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.browserDimension.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(browserDimension, that.browserDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsCommonDimension, browserDimension);
    }

    @Override
    public String toString() {
        return "StatsUserDimension{" +
                "statsCommonDimension=" + statsCommonDimension +
                ", browserDimension=" + browserDimension +
                '}';
    }
}
