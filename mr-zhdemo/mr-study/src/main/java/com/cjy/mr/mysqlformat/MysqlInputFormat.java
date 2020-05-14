package com.cjy.mr.mysqlformat;

import com.sun.tools.corba.se.idl.constExpr.Or;
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

//1.继承InputFormat
public class MysqlInputFormat extends InputFormat<Order, NullWritable> {


    //2.修改切片规则，这里是全加载
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

    //3，定义数据加载规则
    private class RecordReaderMysqlData extends RecordReader<Order, NullWritable> {

        private Connection conn = null; //数据库连接
        private List<Order> list = null; //初始化数据，从mysql中读取数据
        private Order key = null; //每次写出的key
        private NullWritable value = null; //每次写出的v
        private Integer index = 0; //用于终止迭代数据

        public RecordReaderMysqlData() throws IOException, InterruptedException {

        }

        public RecordReaderMysqlData(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            //初始化，由于测试一次数据加载，所以不用切片信息
            //假设没有数据时，null会报错，所以初始化kv
            this.key =  new Order();
            this.value = NullWritable.get();
        }
        //将数据写出
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            //加载数据
            if(list == null){
                list = new ArrayList();
                conn = JDBCUtil.getConnection();
                //从mysql中加载数据
                String sql = "SELECT id,user_code,product_code,value FROM t_order;";
                try {
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

            //将数据写出
            if (index < list.size()) {
                key = list.get(index);
                index++;
                return true;
            }
            //返回结果等于false时，数据读入结束，所以需要list中所有数据取出才行
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

    //重写切片回去函数
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        MySqlInputSplit inputSplit = new MySqlInputSplit();
        splits.add(inputSplit);
        return splits;
    }
    //重写数据加载方式
    @Override
    public RecordReader<Order, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReaderMysqlData(split, context);
    }
}
