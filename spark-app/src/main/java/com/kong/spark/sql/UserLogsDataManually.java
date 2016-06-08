package com.kong.spark.sql;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

/**
 * 模拟生成用户搜索日志
 * Created by kong on 2016/5/5.
 */
public class UserLogsDataManually {
    public static void main(String[] args) {
        long numberItem = 10000;
        generateUserLogs(numberItem, "E:/testData");
    }

    private static void generateUserLogs(long numberItem, String path) {
        StringBuffer userLog = new StringBuffer();
        String fileName = "userLogsForHottest.log";
        //格式:Date,userId,Item,City,Device

        for (int i = 0; i < numberItem; i++) {
            String date = getCountDate(null, "yyyy-MM-dd", -1);
            String userId = generateUserId();
            String item = generateItem();
            String city = generateCity();
            String device = generateDevice();

            userLog.append(date + "\t" + userId + "\t" + item + "\t" + city + "\t" + device + "\n");
            writeLog(path, fileName, userLog + "");
        }
    }

    private static void writeLog(String path, String fileName, String s) {
        FileWriter fw = null;
        PrintWriter pw = null;
        try {
            File file = new File(path + "/" + fileName);
            if (!file.exists()) {
                file.createNewFile();
            } else {
                file.delete();
            }

            fw = new FileWriter(file, true);
            pw = new PrintWriter(fw);

            pw.print(s);
        } catch (Exception e) {
            e.printStackTrace();
            if (pw != null)
                pw.close();
            if (fw != null)
                try {
                    fw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
        } finally {
            if (pw != null)
                pw.close();
            if (fw != null)
                try {
                    fw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
        }
    }

    private static String generateDevice() {
        Random ran = new Random();
        String[] device = {"android", "iphone", "ipad"};
        return device[ran.nextInt(device.length)];
    }

    private static String generateCity() {
        Random ran = new Random();
        String[] city = {"北京", "上海", "广州", "深圳", "重庆", "厦门", "中山", "珠海", "长沙", "武汉"};
        return city[ran.nextInt(city.length)];
    }

    private static String generateItem() {
        Random ran = new Random();
        String[] item = {"小米", "休闲鞋", "洗衣机", "显示器", "显卡", "洗衣液", "行车记录仪"};
        return item[ran.nextInt(7)];
    }

    private static String generateUserId() {
        Random random = new Random();
        String[] userId = {"98415b9c-f3d4-45c3-bc7f-dce3126c6c0b", "7371b4bd-8535-461f-a5e2-c4814b2151e1",
                "49852bfa-a662-4060-bf68-0dddde5feea1", "8768f089-f736-4346-a83d-e23fe05b0ecd",
                "a76ff021-049c-4a1a-8372-02f9c51261d5", "8d5dc011-cbe2-4332-99cd-a1848ddfd65d",
                "a2bccbdf-f0e9-489c-8513-011644cb5cf7", "89c79413-a7d1-462c-ab07-01f0835696f7",
                "8d525daa-3697-455e-8f02-ab086cda7851", "c6f57c89-9871-4a92-9cbe-a2d76cd79cd0",
                "19951134-97e1-4f62-8d5c-134077d1f955", "3202a063-4ebf-4f3f-a4b7-5e542307d726",
                "40a0d872-45cc-46bc-b257-64ad898df281", "b891a528-4b5e-4ba7-949c-2a32cb5a75ec",
                "0d46d52b-75a2-4df2-b363-43874c9503a2", "c1e4b8cf-0116-46bf-8dc9-55eb074ad315",
                "6fd24ac6-1bb0-4ea6-a084-52cc22e9be42", "5f8780af-93e8-4907-9794-f8c960e87d34",
                "692b1947-8b2e-45e4-8051-0319b7f0e438", "dde46f46-ff48-4763-9c50-377834ce7137"
        };
        return userId[random.nextInt(20)];
    }

    /**
     * 获取日期
     *
     * @param date   日期
     * @param format 格式
     * @param i      -1是前一天，1是明天以此类推
     * @return
     */
    private static String getCountDate(String date, String format, int i) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar instance = Calendar.getInstance();
        if (date != null) {
            try {
                instance.setTime(sdf.parse(date));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        instance.add(Calendar.DAY_OF_MONTH, i);
        return sdf.format(instance.getTime());
    }
}
