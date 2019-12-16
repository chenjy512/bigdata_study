package com.cjy.analysis.tool;

import com.cjy.analysis.map.AnalysisTextMap;
import com.cjy.analysis.reducer.AnalysisTextReduce;
import com.cjy.ct.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class AnalysisTextTool implements Tool {

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));
        /**参数介绍 initTableMapperJob(String table, Scan scan, Class<? extends TableMapper> mapper,
         * Class<?> outputKeyClass, Class<?> outputValueClass, Job job)
         * table：表名，读取那张表数据
         * scan：查询条件，读入那些数据
         * mapper：map输入类
         * outputKeyClass：输出key类型
         * outputValueClass：输出value类型
         * job：
         */
        TableMapReduceUtil.initTableMapperJob(Names.TABLE.getValue(),scan,AnalysisTextMap.class,Text.class,Text.class,job);

        job.setReducerClass(AnalysisTextReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //输出到本地，或hdfs都可以
        FileOutputFormat.setOutputPath(job, new Path("D:/out"));

        boolean b = job.waitForCompletion(true);
        if(b){
            return JobStatus.State.SUCCEEDED.getValue();
        }else {
            return JobStatus.State.FAILED.getValue();
        }
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
