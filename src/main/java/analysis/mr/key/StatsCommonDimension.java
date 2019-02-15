package analysis.mr.key;

import analysis.base.BaseDimension;
import analysis.base.DateDimension;
import analysis.base.KpiDimension;
import analysis.base.PlatformDimension;

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
public class StatsCommonDimension extends StatsBaseDimension {
    private DateDimension date = new DateDimension();
    private PlatformDimension platform = new PlatformDimension();
    private KpiDimension kpi = new KpiDimension();

    public StatsCommonDimension(DateDimension date, PlatformDimension platform, KpiDimension kpi) {
        this.date = date;
        this.platform = platform;
        this.kpi = kpi;
    }

    public StatsCommonDimension() {
    }

    public DateDimension getDate() {
        return date;
    }

    public void setDate(DateDimension date) {
        this.date = date;
    }

    public PlatformDimension getPlatform() {
        return platform;
    }

    public void setPlatform(PlatformDimension platform) {
        this.platform = platform;
    }

    public KpiDimension getKpi() {
        return kpi;
    }

    public void setKpi(KpiDimension kpi) {
        this.kpi = kpi;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsCommonDimension s = (StatsCommonDimension) o;
        if (this.date.compareTo(s.date) != 0) {
            return this.date.compareTo(s.date);
        }
        if (this.platform.compareTo(s.platform) != 0) {
            return this.platform.compareTo(s.platform);
        }
        return this.kpi.compareTo(s.kpi);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.platform.write(out);
        this.date.write(out);
        this.kpi.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.platform.readFields(in);
        this.date.readFields(in);
        this.kpi.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsCommonDimension that = (StatsCommonDimension) o;
        return Objects.equals(date, that.date) &&
                Objects.equals(platform, that.platform) &&
                Objects.equals(kpi, that.kpi);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, platform, kpi);
    }

    @Override
    public String toString() {
        return "StatsCommonDimension{" +
                "date=" + date +
                ", platform=" + platform +
                ", kpi=" + kpi +
                '}';
    }
}
