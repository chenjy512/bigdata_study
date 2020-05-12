package com.cjy.mr.inputformat1;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{

    @Override
    protected void map(Text key, BytesWritable value,			Context context)		throws IOException, InterruptedException {
        //key=文件全路径，value=文件所有内容
        context.write(key, value);
    }
}