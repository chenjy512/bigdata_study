package com.cjy.mr.mysqlformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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


public class MysqlInputFormatSplit extends InputFormat<Order, NullWritable> {


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
        //其实就是pageSize
        @Override
        public long getLength() throws IOException, InterruptedException {
            return this.end - this.start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }


    }

    private class RecordReaderMysqlData extends RecordReader<Order, NullWritable> {

        private Connection conn = null;
        private List<Order> list = null; //初始化数据，从mysql中读取数据

        private Integer index = 0; //用于终止迭代数据

        private MySqlInputSplit split;   //分割器，实际就是保存数据范围
        private Order key = null; //每次写出的key
        private NullWritable value = null;       //每次写出的v

        public RecordReaderMysqlData() throws IOException, InterruptedException {

        }

        public RecordReaderMysqlData(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            initialize(split, context);
        }
        //初始化数据
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (MySqlInputSplit)split;
            this.key =  new Order();
            this.value = NullWritable.get();
        }

        //将数据写出
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            //初始化数据
            if(list == null){
                conn = JDBCUtil.getConnection();
                String sql = "SELECT id,user_code,product_code,value FROM t_order LIMIT "+this.split.getStart() +" , "+this.split.getLength();//分页数据
                try {
                    list = new ArrayList<Order>(Integer.parseInt(split.getLength()+""));
                    PreparedStatement pre = conn.prepareStatement(sql);
                    ResultSet resultSet = pre.executeQuery();
                    while (resultSet.next()) {
                        String id = resultSet.getString(1);
                        String usercode = resultSet.getString(2);
                        String productCode = resultSet.getString(3);
                        String value = resultSet.getString(4);
                        Order  order = new Order(id, usercode,productCode,Integer.parseInt(value));
                        list.add(order);
                    }
                    resultSet.close();
                    pre.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            //写出数据
            if (index < list.size()) {
                key = list.get(index);
                index++;
                return true;
            }
            return false;
        }

        @Override
        public Order getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
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

    /**
     * 数据分割器，每次加载指定范围数据
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        //1.获取数据总数
        Integer count = JDBCUtil.queryCount();
        int pageSize = 10; //一次加载一千条数据
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
    public RecordReader<Order, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReaderMysqlData(split, context);
    }
}
