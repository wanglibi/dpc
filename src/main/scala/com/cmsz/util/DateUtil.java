package com.cmsz.util;


import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    public static String getCurrentMin(){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date());
    }

}
