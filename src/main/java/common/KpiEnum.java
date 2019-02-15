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
public enum KpiEnum {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),

    NEW_MEMBER("new_member"),
    NEW_BROWSER_MEMBER("new_browser_member");


    public String name;

    KpiEnum(String name) {
        this.name = name;
    }
}
