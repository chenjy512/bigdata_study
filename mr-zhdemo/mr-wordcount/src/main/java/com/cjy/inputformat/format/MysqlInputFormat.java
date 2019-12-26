package com.cjy.inputformat.format;

import com.cjy.inputformat.bean.Dept;
import com.cjy.util.JDBCUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MysqlInputFormat extends InputFormat<Text, Text> {

    private class RecordReaderMysqlData extends RecordReader<Text, Text>{

        private Connection conn = null;
        private List<Dept> list  = new ArrayList<Dept>();
        private Text key = null;
        private Text value = null;
        private Integer index = 0;
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
             conn = JDBCUtil.getConnection();

             String sql = "SELECT tjdw ,dwmc FROM t_dept;";

            try {
                PreparedStatement pre = conn.prepareStatement(sql);
                ResultSet resultSet = pre.executeQuery();
                while(resultSet.next()){
                    String tjdw = resultSet.getString(1);
                    String dwmc = resultSet.getString(2);
                    Dept dept = new Dept(tjdw, dwmc);
                    list.add(dept);
                }
                resultSet.close();
                pre.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(index < list.size()){
                Dept dept = list.get(index);
                key = new Text(dept.getId());
                value = new Text(dept.getName());
                index++;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }



    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        splits.add(new FileSplit());
        return splits;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReaderMysqlData() ;
    }
}
