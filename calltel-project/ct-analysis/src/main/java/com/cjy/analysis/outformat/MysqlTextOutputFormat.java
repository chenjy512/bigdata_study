package com.cjy.analysis.outformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class MysqlTextOutputFormat extends OutputFormat<Text,Text>{


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
