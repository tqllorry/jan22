package analysis.mr.value;

import common.KpiEnum;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class ReduceOutputValue extends StatsOutputValue{
    private KpiEnum kpiEnum;
    private LongWritable longWritable = new LongWritable();

    public ReduceOutputValue(KpiEnum kpiEnum, LongWritable longWritable) {
        this.kpiEnum = kpiEnum;
        this.longWritable = longWritable;
    }

    public KpiEnum getKpiEnum() {
        return kpiEnum;
    }

    public void setKpiEnum(KpiEnum kpiEnum) {
        this.kpiEnum = kpiEnum;
    }

    public LongWritable getLongWritable() {
        return longWritable;
    }

    public void setLongWritable(LongWritable longWritable) {
        this.longWritable = longWritable;
    }

    public ReduceOutputValue() {
    }

    @Override
    public KpiEnum getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out, kpiEnum);
        this.longWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        WritableUtils.readEnum(in, KpiEnum.class);
        this.longWritable.readFields(in);
    }
}
