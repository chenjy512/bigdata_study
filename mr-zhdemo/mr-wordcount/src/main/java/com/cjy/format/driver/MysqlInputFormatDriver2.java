package com.cjy.format.driver;

import com.cjy.format.informat.MysqlInputFormat;
import com.cjy.format.informat.MysqlInputFormat2;
import com.cjy.format.map.MysqlFormatMapper2;
import com.cjy.format.map.MysqlInputFormatMapper;
import com.cjy.format.reducer.MysqlFormatReducer2;
import com.cjy.format.reducer.MysqlInputFormatReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * driver方式一
 */
public class MysqlInputFormatDriver2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(MysqlInputFormatDriver2.class);

        // 3 设置map和reduce类
        job.setMapperClass(MysqlFormatMapper2.class);
        job.setReducerClass(MysqlFormatReducer2.class);



        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入和输出路径
        //读取mysql中的数据写入到文件中
        job.setInputFormatClass(MysqlInputFormat2.class);
        FileOutputFormat.setOutputPath(job, new Path("d:/out2"));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
