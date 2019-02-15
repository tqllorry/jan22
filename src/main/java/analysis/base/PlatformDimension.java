package analysis.base;

import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

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
public class PlatformDimension extends BaseDimension {
    private int id;
    private String platformName;

    public PlatformDimension() {
    }

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this.id = id;
        this.platformName = platformName;
    }

    public static PlatformDimension getInstance(String platformName) {
        return new PlatformDimension(StringUtils.isEmpty(platformName) ? GlobalConstants.DEFAULT_VALUE : platformName);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PlatformDimension tmp = (PlatformDimension) o;
        if (this.id != tmp.id) {
            return this.id - tmp.id;
        }
        return this.platformName.compareTo(tmp.platformName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformName = in.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, platformName);
    }

    @Override
    public String toString() {
        return "PlatformDimension{" +
                "id=" + id +
                ", platformName='" + platformName + '\'' +
                '}';
    }
}
