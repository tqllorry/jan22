package analysis.mr.new_member;

import analysis.mr.key.StatsUserDimension;
import analysis.mr.value.MapOutPutValue;
import analysis.mr.value.ReduceOutputValue;
import common.KpiEnum;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public class NewMemberReduce extends Reducer<StatsUserDimension, MapOutPutValue, StatsUserDimension, ReduceOutputValue> {
    private static Logger logger = Logger.getLogger(NewMemberReduce.class);
    private static Set set = new HashSet();
    private ReduceOutputValue reduceOutputValue = new ReduceOutputValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutPutValue> values, Context context) throws IOException, InterruptedException {
        for (MapOutPutValue value : values) {
            set.add(value.getId());
        }

        reduceOutputValue.setLongWritable(new LongWritable(set.size()));
        reduceOutputValue.setKpiEnum(KpiEnum.valueOf(key.getStatsCommonDimension().getKpi().getKpiName()));
        context.write(key, reduceOutputValue);

        //清空set集合
        this.set.clear();
    }
}
