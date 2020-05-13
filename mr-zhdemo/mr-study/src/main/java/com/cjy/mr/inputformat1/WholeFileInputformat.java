package com.cjy.mr.inputformat1;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**定义类继承FileInputFormat
 * 重写片数据读取规则-- 一次加载整个文件数据
 */
public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable>{

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        //一次读取一个文件，一个文件就是一片，不进行文件的切片
        return false;
    }

    /**
     * 定义文件读取规则
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)	throws IOException, InterruptedException {
        //使用自定义数据加载方式
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split, context);

        return recordReader;
    }
}