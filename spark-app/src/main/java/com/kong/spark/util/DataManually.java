package com.kong.spark.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * 人造论坛数据，数据格式
 * data：日期，格式yyyy-MM-dd
 * timestamp：时间戳
 * userId：用户Id
 * pageId：页面Id
 * chanelId：板块Id
 * action：点击和注册
 * Created by kong on 2016/4/6 0006.
 */
public class DataManually {

    static String[] channelNames = new String[]{"Spark", "Scala", "Kafka", "Hadoop",
            "Storm", "Hive", "Impala", "Flink", "HBase", "ML", "Streaming"};

    static String[] actionNames = new String[]{"View", "Register"};

    public static void main(String[] args) {
        long numberItems = 5000;
        String path = ".";
        if (args.length > 0) {
            numberItems = Integer.valueOf(args[0]);
            path = args[1];
        }

        System.out.println("User log number is :" + numberItems);

        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);

        Date yesterday = cal.getTime();

        String yesterdayFormat = date.format(yesterday);

        try {
            userLogs(numberItems, path, yesterdayFormat);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static void userLogs(long numberItems, String path, String yesterdayFormat) throws FileNotFoundException {
        StringBuffer userLogBuffer = new StringBuffer("");
        Random ran = new Random();
        for (int i = 0; i < numberItems; i++) {
            long timestamp = new Date().getTime();
            long userId = 0L;
            userId = ran.nextInt((int) numberItems);
            long pageId = ran.nextInt((int) numberItems);
            String channel = channelNames[ran.nextInt(channelNames.length)];
            String action = actionNames[ran.nextInt(actionNames.length)];
            userLogBuffer.append(yesterdayFormat).append("\t").append(timestamp).append("\t")
                    .append(userId).append("\t").append(pageId).append("\t").append(channel).append("\t")
                    .append(action).append("\n");
        }
        System.out.println(userLogBuffer);
        PrintWriter printWriter = null;
        printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+"userLog.log")));
        printWriter.write(userLogBuffer.toString());
        printWriter.close();
    }
}
