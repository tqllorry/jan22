package analysis.service;

import analysis.base.BaseDimension;

import java.io.IOException;

/**
 * Description: 通过维度对象获取维度ID<br/>
 * Copyright(c), 2018, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2018年12月20日
 *
 * @author 唐启亮
 * @version：1.0
 */
public interface IDimension {
    /**
     * 根据dimension的value值获取id<br/>
     * 如果数据库中有，那么直接返回。如果没有，那么进行插入后返回新的id值
     *
     * @param dimension
     * @return
     * @throws IOException
     */
    public int getDimensionIdByObj(BaseDimension dimension) throws IOException;
}
