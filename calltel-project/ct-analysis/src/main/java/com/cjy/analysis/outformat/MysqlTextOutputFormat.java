package com.cjy.analysis.outformat;

import com.cjy.ct.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MysqlTextOutputFormat extends OutputFormat<Text,Text>{


    private  class MysqlRecordWrite extends RecordWriter<Text,Text>{
        //定义对象
        private Connection conn = null;
        private Map<String,Integer> userMap = new HashMap<>();
        private Map<String,Integer> dateMap = new HashMap<>();

        private MysqlRecordWrite(){
            conn = JDBCUtil.getConnection();
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;

            try {
                //获取用户表
                String queryUserSql = "select id, tel from ct_user";
                preparedStatement = conn.prepareStatement(queryUserSql);
                 resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    int id = resultSet.getInt(1);
                    String tel = resultSet.getString(2);
                    userMap.put(tel,id);
                }
                resultSet.close();

                //获取日期表
                String queryDateSql = "select id, year, month, day from ct_date";
                preparedStatement = conn.prepareStatement(queryDateSql);
                 resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    int id = resultSet.getInt(1);
                    String year = resultSet.getString(2);

                    String month = resultSet.getString(3);
                    if(month.length()==1){
                        month = "0"+month;
                    }
                    String day = resultSet.getString(4);
                    if(day.length() == 1){
                        day = "0"+day;
                    }
                    String dateKey = year+month+day;
                    dateMap.put(dateKey,id);
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                //关闭资源
                if(resultSet != null){
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if(preparedStatement != null){
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 实际写出数据的操作
         * @param key    136473848xx_20180101   手机号_日期
         * @param value  12_4536   次数_通话时长
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            //解析kv数据
            String keyStr = Bytes.toString(key.getBytes());
            String valueStr = Bytes.toString(value.getBytes());
            String[] keys = keyStr.split("_");
            String tel = keys[0];
            String date = keys[1];

            String[] vals = valueStr.split("_");
            Integer count = Integer.parseInt(vals[0]);
            Integer sum = Integer.parseInt(vals[1]);

            //封装sql
            String insertSQL = "insert into ct_call ( telid, dateid, sumcall, sumduration ) values ( ?, ?, ?, ? )";
            PreparedStatement pre = null;
            try {
                System.out.println(date);
                pre = conn.prepareStatement(insertSQL);
                pre.setInt(1,userMap.get(tel));
                pre.setInt(2,dateMap.get(date));
                pre.setInt(3,count);
                pre.setInt(4,sum);

                pre.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                //关闭资源
               if(pre != null){
                   try {
                       pre.close();
                   } catch (SQLException e) {
                       e.printStackTrace();
                   }
               }
            }
        }

        /**
         * 关闭资源，既然写入mysql那么必然需要 连接，用完释放
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MysqlRecordWrite();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }



    private FileOutputCommitter committer = null;

    private Path getPath(JobContext job){
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ?null : new Path(name);
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(committer == null){
            Path path = getPath(taskAttemptContext);
            committer = new FileOutputCommitter(path,taskAttemptContext);
        }
        return committer;
    }


    //测试
    public static void main(String[] args) {
        /*MysqlRecordWrite rw = new MysqlRecordWrite();
        Map<String, Integer> dateMap = rw.dateMap;
        Set<Map.Entry<String, Integer>> entries = dateMap.entrySet();
        Iterator<Map.Entry<String, Integer>> iterator = entries.iterator();
        System.out.println(dateMap.size());
        while (iterator.hasNext()){
            Map.Entry<String, Integer> next = iterator.next();
            System.out.println("key:"+next.getKey()+" , value:"+next.getValue());
        }*/
    }
}
