package com.cjy.format.informat;

import com.cjy.format.bean.Dept;
import com.cjy.util.JDBCUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**mysql中的数据格式
 * "8a7f812b6e240760016e240760e10000	TLJ	北京铁路局"
 * "8a7f812b6e240760016e240762a70026	AJP	燕郊"
 * "8a7f812b6e240760016e240762a70050	APP	银城铺"
 * "8a7f812b6e240760016e240762a80090	AQP	阳泉"
 *
 *
 * InputSplit：输入分割，意思就是分几次读入数据。每个InputSplit根据mysql分页坐标来区分。
 *             这里演示一次读取数据，所以简单就是需要这个对象，注意类修饰符不然会报错
 * RecordReaderMysqlData：实际读取数据并写出数据的处理类
 *
 */
public class MysqlInputFormat extends InputFormat<Text, Text> {


    //输入分割
    public static class MySqlInputSplit extends InputSplit implements Writable {

        private long start;
        private long end;

        public MySqlInputSplit() {

        }

        public MySqlInputSplit(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public void setStart(long start) {
            this.start = start;
        }

        public long getEnd() {
            return end;
        }

        public void setEnd(long end) {
            this.end = end;
        }

        //先写出，先读进
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.start);
            out.writeLong(this.end);
        }

        //先写出，先读进
        @Override
        public void readFields(DataInput in) throws IOException {
            this.start = in.readLong();
            this.end = in.readLong();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return this.end - this.start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }


    }

    private class RecordReaderMysqlData extends RecordReader<Text, Text> {

        private Connection conn = null;
        private List<Dept> list = new ArrayList<Dept>(); //初始化数据，从mysql中读取数据
        private Text key = null; //每次写出的key
        private Text value = null; //每次写出的v
        private Integer index = 0; //用于终止迭代数据

        public RecordReaderMysqlData() throws IOException, InterruptedException {

        }

        public RecordReaderMysqlData(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            conn = JDBCUtil.getConnection();

            //从mysql中加载数据
            String sql = "SELECT tjdw ,dwmc FROM t_dept;";
            try {
                PreparedStatement pre = conn.prepareStatement(sql);
                ResultSet resultSet = pre.executeQuery();
                while (resultSet.next()) {
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
        //将数据写出
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (index < list.size()) {
                Dept dept = list.get(index);
                key = new Text(dept.getId());
                value = new Text(dept.getName());
                index++;
                return true;
            }
            //返回结果等于false时，数据读入结束，所以需要list中所有数据取出才行
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
        //关闭资源
        @Override
        public void close() throws IOException {
            if (conn != null) {
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
        MySqlInputSplit inputSplit = new MySqlInputSplit();
        splits.add(inputSplit);
        return splits;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReaderMysqlData(split, context);
    }
}
