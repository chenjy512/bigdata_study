package com.cjy.wc.driver;

import com.cjy.wc.map.WordcountMapper;
import com.cjy.wc.reducer.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * driver方式二
 */
public class WordcountTool implements Tool {

    @Override
    public int run(String[] args) throws Exception {


        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(WordcountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    private Configuration configuration = null;
    @Override
    public void setConf(Configuration conf) {
            this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }


    public static void main(String[] args) {
        args = new String[]{"d:/tmp/hello.txt","d:/out"};
        try {
            int result = ToolRunner.run( new WordcountTool(), args );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
