package com.cjy.analysis.map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.soap.Text;
import java.io.IOException;

public class AnalysisTextMap extends TableMapper<Text,Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String s = Bytes.toString(key.get());
        System.out.println("map:"+s);
    }
}