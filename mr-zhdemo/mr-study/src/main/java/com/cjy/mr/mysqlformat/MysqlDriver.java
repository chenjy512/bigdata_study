package com.cjy.mr.mysqlformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class MysqlDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(MysqlDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(MysqlMapper.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
        //读取mysql中的数据写入到文件中
//        job.setInputFormatClass(MysqlInputFormat.class);
        //带切片格式
        job.setInputFormatClass(MysqlInputFormatSplit.class);

        FileOutputFormat.setOutputPath(job, new Path("d:/out2"));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
