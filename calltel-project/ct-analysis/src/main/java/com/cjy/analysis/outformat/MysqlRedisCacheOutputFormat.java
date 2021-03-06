package com.cjy.analysis.outformat;

import com.cjy.ct.util.JDBCUtil;
import com.cjy.ct.util.RedisCliUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlRedisCacheOutputFormat extends OutputFormat<Text, Text> {


    private class MysqlRedisRecordWriter extends RecordWriter<Text,Text>{

        private Jedis jedis = null;
        private Connection conn = null;

        public MysqlRedisRecordWriter(){
            //初始化连接与缓存对象
            jedis = RedisCliUtil.getJedis();
            conn = JDBCUtil.getConnection();
        }

        /**
         * 实际写出数据的操作
         * @param key    136473848xx_20180101   手机号_日期
         * @param val  12_4536   次数_通话时长
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(Text key, Text val) throws IOException, InterruptedException {
            //15133295266_201802 --> 15133295266_20180231
//            String keyStr = Bytes.toString(key.getBytes());
            String valStr = Bytes.toString(val.getBytes());
            String s = key.toString();
            String[] s1 = s.split("_");
            String[] s2 = valStr.split("_");

            String tel = s1[0];
            String date = s1[1];
            String count = s2[0];
            String sum = s2[1];

            //封装sql
            String insertSQL = "insert into ct_call ( telid, dateid, sumcall, sumduration ) values ( ?, ?, ?, ? )";
            PreparedStatement pre = null;
            try {
//                System.out.println(date);
                pre = conn.prepareStatement(insertSQL);

                String telId = jedis.hget("ct_tel", tel);
                String dateId = jedis.hget("ct_date", date);
                if(telId == null){
                    System.out.println("tel is null :"+tel);
                }
                if(dateId == null){
                    System.out.println("dateId is null :"+date);
                }
                if(dateId.equals("20180230")){
                    System.out.println(s);
                }
//                System.out.println("tel:"+tel+" ct_date:"+date);
                pre.setInt(1, Integer.parseInt(telId));
                pre.setInt(2, Integer.parseInt(dateId));
                pre.setInt(3, Integer.parseInt(count));
                pre.setInt(4, Integer.parseInt(sum));

                pre.executeUpdate();
            }catch (Exception e){
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

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(jedis != null){
                jedis.close();
            }
        }
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MysqlRedisRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }
    //定义输出提交对象
    private FileOutputCommitter committer = null;

    //获取路径
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

    public static void main(String[] args) {
        String s = "15133295266_201802";
        Text text = new Text(s);
        String s1 = Bytes.toString(text.getBytes());
        System.out.println(s1);
    }
}
