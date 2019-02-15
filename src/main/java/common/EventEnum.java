package common;

/**
 * Description: xxx<br/>
 * Copyright(c), 2019, tqllorry <br/>
 * This program is protected by copyright laws. <br/>
 * Date: 2019年01月23日
 *
 * @author 唐启亮
 * @version：1.0
 */
public enum EventEnum {
    LANUCH(1, "lanuch event", "e_l"),
    PAGEVIEW(2, "pageview event", "e_pv"),
    EVENT(3, "event name", "e_e"),
    CHARGEREQUEST(4, "charge request event", "e_crt"),
    CHARGESUCCESS(5, "charge success", "e_cs"),
    CHARGEREFUND(6, "charge refund", "e_cr");
    public int id;
    public String name;
    public String alias;

    EventEnum(int id, String name, String alias) {
        this.id = id;
        this.name = name;
        this.alias = alias;
    }

    //根据别名获取对应的枚举类型
    public static EventEnum valuesOfAlias(String alia) {
        for (EventEnum event : values()) {
            if (event.alias.equals(alia)) {
                return event;
            }
        }
        throw new RuntimeException("没有对应的事件类型");
    }
}
