package analysis.base;

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
public class KpiDimension extends BaseDimension {
    private int id;
    private String KpiName;

    public KpiDimension(int id, String kpiName) {
        this.id = id;
        KpiName = kpiName;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        KpiDimension k = (KpiDimension) o;
        if (this.id != k.id) {
            return this.id - k.id;
        }
        return this.KpiName.compareTo(k.KpiName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.KpiName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.KpiName = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KpiDimension that = (KpiDimension) o;
        return id == that.id &&
                Objects.equals(KpiName, that.KpiName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, KpiName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return KpiName;
    }

    public void setKpiName(String kpiName) {
        KpiName = kpiName;
    }

    public KpiDimension() {
    }

    public KpiDimension(String kpiName) {
        KpiName = kpiName;
    }

    @Override
    public String toString() {
        return "KpiDimension{" +
                "id=" + id +
                ", KpiName='" + KpiName + '\'' +
                '}';
    }
}
