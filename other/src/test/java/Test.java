import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by lenovo on 2016/6/13.
 */
public class Test {
    public static void main(String[] args) throws ParseException {
        String date = "20160612";
        String time = "110543";
        String date1 = "20160612";
        String time1 = "111043";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        long dateTime = sdf.parse(date + "-" + time).getTime();
        long dateTime1 = sdf.parse(date1 + "-" + time1).getTime();
        System.out.println(dateTime1-dateTime);
    }
}
