package com.cjy.mr.outputformat;
import java.io.IOException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FilterOutputFormat extends FileOutputFormat<Text, NullWritable>{

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)			throws IOException, InterruptedException {

        // 创建一个RecordWriter
        return new FilterRecordWriter(job);
    }

    class FilterRecordWriter extends RecordWriter<Text, NullWritable>{

        //1.定义输出流
        FSDataOutputStream errorOut = null;
        FSDataOutputStream sqlOut = null;

        public FilterRecordWriter(TaskAttemptContext job) {

            // 2 获取文件系统
            FileSystem fs;

            try {
                fs = FileSystem.get(job.getConfiguration());
                // 3 创建输出文件路径
                    Path sqlPath = new Path("d:/log/sql.log");
                Path errorPath = new Path("d:/log/error.log");

                // 4 创建输出流
                errorOut = fs.create(errorPath);
                sqlOut = fs.create(sqlPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            //5. 判断是否包含“atguigu”输出到不同文件
            if (key.toString().contains("==>  Preparing:")) {
                sqlOut.write(key.toString().getBytes());
            } else if(key.toString().contains("Caused by:")){
                errorOut.write(key.toString().getBytes());
            }else{
                //其它信息舍弃，其实在数据输入的时候就可以做此种判断，但是这里为了演示outputformat
            }
        }
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // 关闭资源
            IOUtils.closeStream(sqlOut);
            IOUtils.closeStream(errorOut);	}
    }
}