package com.cjy.mrtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *设置驱动类
 */
public class FruitDriver
        implements Tool
{
    private Configuration configuration = null;

    public int run(String[] args)
            throws Exception
    {
        //1.创建job任务
        Job job = Job.getInstance(this.configuration);
        //2. 关联驱动类
        job.setJarByClass(FruitDriver.class);
        //3. 设置mapper与输入输出类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4. 设置reduce，args[1]表名
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);
        //5. 设置数据文件路径
        FileInputFormat.setInputPaths(job, new Path[] { new Path(args[0]) });
        //6. 返回值
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration conf)
    {
        this.configuration = conf;
    }

    public Configuration getConf()
    {
        return this.configuration;
    }

    public static void main(String[] args)
    {
        try
        {   //创建配置
            Configuration configuration = new Configuration();
            //传入配置运行任务
            int run = ToolRunner.run(configuration, new FruitDriver(), args);
            System.exit(run);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
