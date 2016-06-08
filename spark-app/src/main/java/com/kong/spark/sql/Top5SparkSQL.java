package com.kong.spark.sql;

/**
 * 以京东为例，找出搜索平台用户每天排名前5名的产品
 * 元数据：Date、UserId、Item、City、Device
 * 1.过滤数据，ETL（进行广播）
 * 2.过滤后的数据以Parquet方式存在
 * 3.对过滤后的目标数据进行指定条件查询
 * 4.由于商品是分为种类的，我们在得出最终结果之前，首先对商品进行UV，PV
 *   此时我们要对商品进行UV计算的话，必须构建k-v的RDD，例如过程为（date#Item，UserId）以方便进行groupByKey
 *   groupByKey之后对user进行去重，并计算出每一天每一种商品的UV，最终计算出来的结果数据类型（date#Item，UV）
 * 5.使用开窗函数row_number统计出每日商品UV前5名的内容，row_number() over (partitioned by date order by uv desc) rank
 *   此时会产生以date、item、uv为Row的DataFrame
 * 6.DataFrame转成RDD，根据日期进行分组并分析出每天排名前5为热搜Item
 * 7.进行k-v交换，然后进行sortByKey进行点击热度排名
 * 8.再次进行k-v交换，得出目标数据（达特#Item，UV）的格式
 * 9.通过RDD直接操作Mysql等把结果放入生产系统中的DB中，再通过J2EE可视化
 *   也可以放在Hive中。也可以放在SparkSQL中，通过Thrift技术供Java EE使用
 *   当然秒杀系统，Redis中。
 * Created by kong on 2016/4/11 0011.
 */
public class Top5SparkSQL {
    public  static void main(String[] args){

    }
}
