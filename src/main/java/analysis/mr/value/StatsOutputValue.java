package analysis.mr.value;

import common.KpiEnum;
import org.apache.hadoop.io.Writable;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public abstract class StatsOutputValue implements Writable {
    public abstract KpiEnum getKpi();
}
