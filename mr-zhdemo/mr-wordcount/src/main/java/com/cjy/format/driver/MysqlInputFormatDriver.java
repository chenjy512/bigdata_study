package com.cjy.format.driver;

import com.cjy.format.format.MysqlInputFormat;
import com.cjy.format.map.MysqlInputFormatMapper;
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
public class MysqlInputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(MysqlInputFormatDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(MysqlInputFormatMapper.class);
        job.setReducerClass(MysqlInputFormatReducer.class);

        job.setInputFormatClass(MysqlInputFormat.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("d:/out2"));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
