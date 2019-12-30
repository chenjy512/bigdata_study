package com.cjy.hbase.mr3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *设置驱动类--从hbase中读取数据存入mysql中
 */
public class FruitDriver3
        implements Tool
{
    private Configuration configuration = null;

    public int run(String[] args)
            throws Exception
    {
        //1.创建job任务
        Job job = Job.getInstance(this.configuration);
        //2. 关联驱动类
        job.setJarByClass(FruitDriver3.class);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        /**参数介绍 initTableMapperJob(String table, Scan scan, Class<? extends TableMapper> mapper,
         * Class<?> outputKeyClass, Class<?> outputValueClass, Job job)
         * table：表名，读取那张表数据
         * scan：查询条件，读入那些数据
         * mapper：map输入类
         * outputKeyClass：输出key类型
         * outputValueClass：输出value类型
         * job：
         */
        TableMapReduceUtil.initTableMapperJob("fruit",scan,FruitMapper3.class,Text.class,Text.class,job);

        job.setReducerClass(FruitReducer3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //写入到mysql 中
        job.setOutputFormatClass(MysqlOutputFormat.class);
        //写入到本地文件--也可以写入到hdfs需要有yarn来执行jar
//        FileOutputFormat.setOutputPath(job, new Path("d:/out3/"));
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
        {
            //创建配置
            Configuration configuration = new Configuration();
//需要 hbase-site.xml 可在本地运行
//          Configuration configuration = HBaseConfiguration.create();
            //传入配置运行任务
            int run = ToolRunner.run(configuration, new FruitDriver3(), args);
            System.exit(run);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
