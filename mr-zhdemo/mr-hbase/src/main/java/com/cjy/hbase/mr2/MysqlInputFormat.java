package com.cjy.hbase.mr2;


import com.cjy.hbase.Util.JDBCUtil;
import com.cjy.hbase.bean.Fruit;
import org.apache.hadoop.io.LongWritable;
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
 *
 * InputSplit：输入分割，意思就是分几次读入数据。每个InputSplit根据mysql分页坐标来区分。
 *             这里演示一次读取数据，所以简单就是需要这个对象，注意类修饰符不然会报错
 * RecordReaderMysqlData：实际读取数据并写出数据的处理类
 *
 */
public class MysqlInputFormat extends InputFormat<LongWritable, Text> {


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

    private class RecordReaderMysqlData extends RecordReader<LongWritable, Text> {

        private Connection conn = null;
        private List<Fruit> list = null; //初始化数据，从mysql中读取数据
        private LongWritable key = null; //每次写出的key
        private Text value = null; //每次写出的v
        private Integer index = 0; //用于终止迭代数据
        private MySqlInputSplit split;

        public RecordReaderMysqlData() throws IOException, InterruptedException {

        }

        public RecordReaderMysqlData(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (MySqlInputSplit)split;
            this.key =  new LongWritable();
            this.value = new Text();
        }
        //将数据写出
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            //初始化数据
            if(list == null){
                conn = JDBCUtil.getConnection();
                list = new ArrayList<Fruit>(Integer.parseInt(split.getLength()+""));
                //从mysql中加载数据
                String sql = "SELECT id,NAME,color FROM fruit LIMIT "+this.split.getStart() +" , "+this.split.getLength();//分页数据
                try {
                    PreparedStatement pre = conn.prepareStatement(sql);
                    ResultSet resultSet = pre.executeQuery();
                    while (resultSet.next()) {
                        String id = resultSet.getString(1);
                        String name = resultSet.getString(2);
                        String color = resultSet.getString(3);
                        Fruit fruit = new Fruit(id,name,color);
                        list.add(fruit);
                    }
                    resultSet.close();
                    pre.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            //写出数据
            if (index < list.size()) {
                Fruit bean = list.get(index);
                key.set(Long.parseLong(bean.getId()));  //数据坐标
                value.set(bean.toString());   //对象字符串 id+"\t"+dwmc + "\t"+ code + "\t" + value;
                index++;
                return true;
            }

            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
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
        //1.获取数据总数
        Integer count = JDBCUtil.queryCount();
        int pageSize = 1000; //一次加载一千条数据
        //2. 获取数据页数
        int total = count % pageSize == 0?count / pageSize :count / pageSize+1;
        //3. 根据数据页数设置分割器起始坐标
        for (int i = 0 ;i < total;i++){
            int start = i*pageSize;
            int end = start + pageSize;
            MySqlInputSplit inputSplit = new MySqlInputSplit(start,end);
            splits.add(inputSplit);
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReaderMysqlData(split, context);
    }
}
