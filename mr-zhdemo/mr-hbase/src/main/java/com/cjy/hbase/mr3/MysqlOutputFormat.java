package com.cjy.hbase.mr3;

import com.cjy.hbase.Util.JDBCUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<Text,Text> {

        public MysqlOutputFormat(){

        }

    /**
     * 实际写入mysql操作
     */
    public static class MySqlRecordWriter extends RecordWriter<Text,Text>{
        private Connection conn = null;
        public MySqlRecordWriter(){
            //初始化连接
            conn = JDBCUtil.getConnection();
        }
        //写入数据
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            String sql = "INSERT INTO fruit(id,name,color) VALUES(?,?,?);";
            String k = key.toString();
            String v = value.toString();
            String[] split = v.split("_");
            PreparedStatement pre = null;
            try {
                pre = conn.prepareStatement(sql);
                pre.setString(1,split[0]);
                pre.setString(2,split[1]);
                pre.setString(3,split[2]);
                pre.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                if(pre != null){
                    try {
                        pre.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        //关闭资源
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //使用自定义数据写入操作
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }
    //写出提交
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, context);
    }
}
