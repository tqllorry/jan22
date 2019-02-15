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
public class BrowserDimension extends BaseDimension {
    private int id; // id7
    private String browserName; // 名称
    private String browserVersion; // 版本

    public BrowserDimension() {
        super();
    }

    public BrowserDimension(String browserName, String browserVersion) {
        super();
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    public void clean() {
        this.id = 0;
        this.browserName = "";
        this.browserVersion = "";
    }

    public static BrowserDimension getInstance(String browserName, String browserVersion) {
        BrowserDimension b = new BrowserDimension();
        b.browserName = browserName;
        b.browserVersion = browserVersion;
        return b;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        BrowserDimension o1 = (BrowserDimension) o;
        if (this.id != o1.id) {
            return this.id - o1.id;
        } else if (!this.browserName.equals(o1.browserName)) {
            return this.browserName.compareTo(o1.browserName);
        } else {
            return this.browserVersion.compareTo(o1.browserVersion);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.browserName);
        out.writeUTF(this.browserVersion);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.browserName = in.readUTF();
        this.browserVersion = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browserVersion, that.browserVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, browserName, browserVersion);
    }

    @Override
    public String toString() {
        return "BrowserDimension{" +
                "id=" + id +
                ", browserName='" + browserName + '\'' +
                ", browserVersion='" + browserVersion + '\'' +
                '}';
    }
}
