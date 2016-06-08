package com.kong.redis.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * 时间工具
 * Created by kong on 2016/4/30.
 */
public class DateUtil {
    public static final String Format_yyyy_MM_dd = "yyyy-MM-dd";
    public static final String Format_yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";

    /**
     * 当前的时间 yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    public static String getCurrentTimestamp() {
        SimpleDateFormat format = new SimpleDateFormat(Format_yyyy_MM_dd_HH_mm_ss);
        Date today = new Date();
        return format.format(today);
    }

    /**
     * Date对象转换成字符串
     *
     * @param date
     * @param format 默认"yyyy-MM-dd HH:mm:ss"
     * @return
     */
    public static String dateToString(Date date, String format) {
        if (date == null)
            return "";
        SimpleDateFormat formatDate = new SimpleDateFormat(StringUtil.isNull(format) ? Format_yyyy_MM_dd_HH_mm_ss : format.trim());
        return formatDate.format(date);
    }

    /**
     * Date对象转换成字符串
     *
     * @param date
     * @return
     */
    public static String dateToString(Date date) {
        return dateToString(date, null);
    }

    /**
     * 字符串对象转换成Date
     *
     * @param timeStr
     * @param format  默认"yyyy-MM-dd HH:mm:ss"
     * @return
     */
    public static Date strToDate(String timeStr, String format) {
        if (StringUtil.isNull(timeStr))
            return null;
        SimpleDateFormat formatDate = new SimpleDateFormat(StringUtil.isNull(format) ? Format_yyyy_MM_dd_HH_mm_ss : format.trim().replace(".", "-").replace("/", "-"));

        try {
            return formatDate.parse(timeStr);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Date strToDate(String timeStr){
        return strToDate(timeStr,null);
    }

    /**
     * 得到系统日期
     *
     * @return
     */
    public static String getDate() {
        Calendar calendar = Calendar.getInstance();
        String year = calendar.get(Calendar.YEAR) + "";
        String month = calendar.get(Calendar.MONTH) + 1 + "";
        String day=calendar.get(Calendar.DAY_OF_MONTH)+"";
        if (month.length() == 1)
            month = "0" + month;
        if (day.length() == 1)
            day = "0" + day;

        return year+"-"+month+"-"+day;
    }

    /**
     * 得到系统日期,xx月xx日 xx xx:xx
     *
     * @return
     */
    public static String getWapDate() {
        Calendar calendar = Calendar.getInstance();
        String month = calendar.get(Calendar.MONTH) + 1 + "";
        String day = calendar.get(Calendar.DAY_OF_MONTH)+"";
        String hour = calendar.get(Calendar.HOUR_OF_DAY)+"";
        String minute = calendar.get(Calendar.MINUTE)+"";

        if (month.length() == 1)
            month = "0" + month;

        return month+"月"+day+"日 "+hour+":"+minute;
    }


    /**
     * 得到一个时间延后或前移几天的时间,nowdate为时间,delay为前移或后延的天数
     * @param nowdate
     * @param delay
     * @return
     */
    public static String getNextDay(String nowdate, int delay) {
        try{
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            String mdate = "";
            Date d = strToDate(nowdate,"yyyy-MM-dd");
            long myTime = (d.getTime() / 1000) +  delay * 24 * 60 * 60;
            d.setTime(myTime * 1000);
            mdate = format.format(d);
            return mdate;
        }catch(Exception e){
            return "";
        }
    }


    /**
     *
     * 得到一个时间延后或前移几天的时间,nowdate为时间,delay为前移或后延的天数
     * @param nowdate
     * @param delay  小于 0，过去多小天，大于0 未来多小天
     * @param dateFormat
     * @return
     */
    public static String getNextDay(String nowdate,int delay,String dateFormat) {
        try{
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            String mdate = "";
            Date d = strToDate(nowdate,dateFormat);
            long myTime = (d.getTime() / 1000) + delay* 24 * 60 * 60;
            d.setTime(myTime * 1000);
            mdate = format.format(d);
            return mdate;
        }catch(Exception e){
            return "";
        }
    }


    /**
     * 返回今天的时间段
     * @return   yyyy-MM-dd HH:mm:ss
     */
    public static String[] getTodayPeriods(){
        String today = dateToString(new Date(), Format_yyyy_MM_dd);
        return new String[]{today+" 00:00:00",today+" 23:59:59"};
    }

    /**
     * 返回昨天的时间段
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String[] getYesterdayPeriods(){
        String today = dateToString(new Date(),Format_yyyy_MM_dd);
        String beforeDay = getNextDay(today,-1,Format_yyyy_MM_dd);
        return new String[]{beforeDay+" 00:00:00",beforeDay+" 23:59:59"};
    }

    /**
     * 返回一个星期(7天前)的时间段(含今天)
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String[] getWeekPeriods(){
        String today = dateToString(new Date(),Format_yyyy_MM_dd);
        String beforeDay = getNextDay(today,-6,Format_yyyy_MM_dd);
        return new String[]{beforeDay+" 00:00:00",today+" 23:59:59"};
    }


    /**
     * 返回的一个月时间段(含今天)
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String[] getMonthDayPeriods(int difMonths){
        difMonths=difMonths==0?1:difMonths;
        String today = dateToString(new Date(),Format_yyyy_MM_dd);
        String beforeDay = getNextDay(today,-7*4*difMonths-1,Format_yyyy_MM_dd);
        return new String[]{beforeDay+" 00:00:00",today+" 23:59:59"};
    }



    /**
     * 得到系统当前的时间
     * @return
     */
    public static String getSystemTime(){
        return dateToString(new Date(),Format_yyyy_MM_dd_HH_mm_ss);
    }

    /**
     * 将calendar的星期几转化为我们习惯的（1-星期一，7-星期日）
     * @param dayOfWeek
     * @return
     */
    public static long toChineseWeek(long dayOfWeek){
        return dayOfWeek-1==0?7:dayOfWeek-1;
    }

    /**
     * 获取当前日期是星期几
     *
     * @param dt
     * @return 当前日期是星期几
     */
    public static String getWeekStr(Date dt){
        String[] weekDays = {"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0)
            w = 0;
        return weekDays[w];
    }


    /**
     * 验证字符串是否是合法的日期
     * @param dateStr
     * @param format
     * @return
     */
    public static boolean isValidDate(String dateStr,String format){
        try{
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            dateFormat.setLenient(false);
            dateFormat.parse(dateStr);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 返回当月的起始 日期  2011-01-01 : 2011-01-31
     * @return
     */
    public static String[] getMonthPeriods(int difMonth){
        Date today = new Date();
        int year = today.getYear()+1900;
        int month = today.getMonth()+difMonth + 1;
        Calendar c = new GregorianCalendar(today.getYear(),today.getMonth()+difMonth,today.getDate());
        return new String[]{year+"-"+month+"-01",year+"-"+month+"-"+c.getActualMaximum(Calendar.DATE)};
    }
    /**
     * 取得当前时间,long格式的毫秒数
     * @return
     */
    public static long getSysTime(){
        Date today = new Date();
        return today.getTime();
    }

}
