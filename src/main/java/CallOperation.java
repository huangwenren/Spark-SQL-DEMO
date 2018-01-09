import edu.tongji.entity.Call;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


public class CallOperation {
    public static void main(String[] args) {
        // cluster
        // 设置partitions可以设置task的数量，从而减少保存结果时小文件的数量，spark sql partitions默认值为200
        SparkSession spark = SparkSession.builder().config("spark.sql.shuffle.partitions", 10).appName("Call Operation").getOrCreate();

        // local
        // SparkSession spark = SparkSession.builder().master("local").appName("Call Operation").getOrCreate();

        JavaRDD<Call> callRDD = spark.read().textFile("hdfs://master:9000/tb_call_201202_random.txt").javaRDD().map(line -> {
            String[] parts = line.split("\t");
            Call call = new Call();
            call.setDay_id(Integer.parseInt(parts[0]));
            call.setCalling_nbr(parts[1]);
            call.setCalled_nbr(parts[2]);
            call.setCalling_optr(Integer.parseInt(parts[3]));
            call.setCalled_optr(Integer.parseInt(parts[4]));
            call.setCalling_city(parts[5]);
            call.setCalled_city(parts[6]);
            call.setCalling_roam_city(parts[7]);
            call.setCalled_roam_city(parts[8]);
            call.setStart_time(parts[9]);
            call.setEnd_time(parts[10]);
            call.setRaw_dur(Integer.parseInt(parts[11]));
            call.setCall_type(Integer.parseInt(parts[12]));
            call.setCalling_cell(parts[13]);
            return call;
        });

        Dataset<Row> callDF = spark.createDataFrame(callRDD, Call.class);
        callDF.createOrReplaceTempView("call");


        // 第一题
        spark.sql("select calling_nbr, round(count(*)/count(distinct day_id), 2) as avg_calls from call group by calling_nbr").write().csv("hdfs://master:9000/results/q1");


        // 第二题
        // q2-1 计算各个运营商在各个通话类型的占比——（主叫）
        spark.sql("select count(*) as total, call_type from call group by call_type").createOrReplaceTempView("tmp1");
        spark.sql("select count(*) as sub_total, call_type, calling_optr from call group by call_type, calling_optr").createOrReplaceTempView("tmp2");
        spark.sql("select round(sub_total/total*100, 2) as rate, tmp1.call_type, calling_optr from tmp1, tmp2 where tmp1.call_type = tmp2.call_type").write().json("hdfs://master:9000/results/q2-1");

        // q2-2 计算各个运营商在各个通话类型的占比——（被叫）
        spark.sql("select count(*) as sub_total, call_type, called_optr from call group by call_type, called_optr").createOrReplaceTempView("tmp3");
        spark.sql("select round(sub_total/total*100, 2) as rate, tmp1.call_type, called_optr from tmp1, tmp3 where tmp1.call_type = tmp3.call_type").write().json("hdfs://master:9000/results/q2-2");


        // 第三题
        // 获取结束时间的
        spark.udf().register("endTime", (String start_time, Integer raw_dur) -> {
            String[] start_parts = start_time.split(":");

            Integer tmp_second = Integer.parseInt(start_parts[2]) + raw_dur;
            Integer second = tmp_second % 60;
            Integer tmp_minute = Integer.parseInt(start_parts[1]) + tmp_second / 60;
            Integer minute = tmp_minute % 60;
            Integer tmp_hour =  Integer.parseInt(start_parts[0]) + tmp_minute / 60;
            Integer hour;
            if (tmp_hour < 24) {
                hour = tmp_hour;
            }else {
                hour = tmp_hour % 24;
            }

            return String.format("%s:%s:%s", hour, minute, second);

        }, DataTypes.StringType);

        // 获取指定时间段内的通话时长
        spark.udf().register("rangeDuration", (String start_time, String end_time ,Integer raw_dur, Integer time_range) -> {
            String[] start_parts = start_time.split(":");
            String[] end_parts = end_time.split(":");

            Integer start_hour = Integer.parseInt(start_parts[0]);
            Integer end_hour = Integer.parseInt(end_parts[0]);

            Integer start_time_range = start_hour / 3 + 1;
            Integer end_time_range = end_hour / 3 + 1;

            // 相同时间段
            if (start_time_range ==  end_time_range && start_time_range == time_range) {
                return raw_dur;
            }

            // 不同时间段,开始时间在指定时间段
            if (start_time_range != end_time_range && start_time_range == time_range) {
                Integer sub_hour = (time_range * 3 - 1) - Integer.parseInt(start_parts[0]);
                Integer sub_minute = 59 - Integer.parseInt(start_parts[1]);
                Integer sub_second = 59 - Integer.parseInt(start_parts[2]);

                return sub_hour * 3600 + sub_minute * 60 + sub_second;
            }

            // 不同时间段，结束时间在指定时间段
            if (start_time_range != end_time_range && end_time_range == time_range) {
                Integer sub_hour = Integer.parseInt(end_parts[0]) - ((time_range - 1) * 3);
                Integer sub_minute = Integer.parseInt(end_parts[1]) - 0;
                Integer sub_second = Integer.parseInt(end_parts[2]) - 0;

                return sub_hour * 3600 + sub_minute * 60 + sub_second;
            }


            // 不同时间段，但是开始时间和结束时间中间包含了指定的时间段，因此返回 3600*3=10800
            if (start_time_range < time_range && time_range < end_time_range){
               return 10800;
            }

            // 没有包含指定时间段的通话则返回0
            return 0;

        }, DataTypes.IntegerType);

        // 计算出正确结束时间
        spark.sql("select *, endTime(start_time, raw_dur) as new_end_time from call").createOrReplaceTempView("new_call");

        // 计算每个时间段的通话时长
        spark.sql("select calling_nbr," +
                "rangeDuration(start_time, new_end_time, raw_dur, 1) as range1, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 2) as range2, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 3) as range3, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 4) as range4, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 5) as range5, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 6) as range6, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 7) as range7, " +
                "rangeDuration(start_time, new_end_time, raw_dur, 8) as range8 " +
                "from new_call").createOrReplaceTempView("range_dur");

        // 计算出在每个单独时间段总的通话时长
        spark.sql("select calling_nbr, " +
                "sum(range1) as range1_dur, " +
                "sum(range2) as range2_dur, " +
                "sum(range3) as range3_dur, " +
                "sum(range4) as range4_dur, " +
                "sum(range5) as range5_dur, " +
                "sum(range6) as range6_dur, " +
                "sum(range7) as range7_dur, " +
                "sum(range8) as range8_dur  " +
                "from range_dur group by calling_nbr").createOrReplaceTempView("range_subtotal");

        // 计算出每个用户总的通话时长
        spark.sql("select calling_nbr, sum(raw_dur) as total from call group by calling_nbr").createOrReplaceTempView("range_total");

        // 计算每个时间段占用户总通话时长的百分比
        spark.sql("select range_subtotal.calling_nbr, " +
                "round(range1_dur/total*100, 2) as range1_rate, " +
                "round(range2_dur/total*100, 2) as range2_rate, " +
                "round(range3_dur/total*100, 2) as range3_rate, " +
                "round(range4_dur/total*100, 2) as range4_rate, " +
                "round(range5_dur/total*100, 2) as range5_rate, " +
                "round(range6_dur/total*100, 2) as range6_rate, " +
                "round(range7_dur/total*100, 2) as range7_rate, " +
                "round(range8_dur/total*100, 2) as range8_rate " +
                "from range_subtotal, range_total where range_subtotal.calling_nbr = range_total.calling_nbr").write().csv("hdfs://master:9000/results/q3");
    }

}
