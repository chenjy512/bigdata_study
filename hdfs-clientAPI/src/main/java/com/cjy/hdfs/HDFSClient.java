package com.cjy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class HDFSClient {

    @Before
    public void before(){
        //设置访问用户
        Properties pro = System.getProperties();
        pro.setProperty("HADOOP_USER_NAME","root");
        // 权限不足引起，设置指定用户即可 https://www.cnblogs.com/413xiaol/p/9949936.html
        // org.apache.hadoop.security.AccessControlException: Permission denied: user=chenjunying, access=WRITE, inode="/test":root:supergroup:drwxr-xr-x
    }

    //1. 创建文件夹
    @Test
    public void testMkdir() throws URISyntaxException, IOException {


        //创建配置
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf);
        //行动
        boolean b = fs.mkdirs(new Path("/test2"));
        if(b)
            System.out.println("succes");
        else
            System.out.println("error");

        fs.close();

         }


         //测试文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("/Users/chenjunying/Downloads/1.txt"), new Path("/test2/"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
        //参数优先级排序： （1）客户端代码中设置的值 >（2）classpath下的用户自定义配置文件 >（3）然后是服务器的默认配置
    }

        //文件下载

        @Test
        public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 执行下载操作
            // boolean delSrc 指是否将原文件删除
            // Path src 指要下载的文件路径
            // Path dst 指将文件下载到的路径
            // boolean useRawLocalFileSystem 是否开启文件校验
            fs.copyToLocalFile(false, new Path("/test2/1.txt"), new Path("/Users/chenjunying/Downloads/1.txt"), true);
            // 3 关闭资源
            fs.close();
        }

        //文件夹删除--包含文件夹下的文件整个移除
        @Test
        public void testDelete() throws IOException, InterruptedException, URISyntaxException{
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 执行删除
            fs.delete(new Path("/test/"), true);

            // 3 关闭资源
            fs.close();
        }


        //文件名修改
        @Test
        public void testRename() throws IOException, InterruptedException, URISyntaxException{
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 修改文件名称
            fs.rename(new Path("/test2/1.txt"), new Path("/test2/2.txt"));

            // 3 关闭资源
            fs.close();
        }


    //查看文件名称、权限、长度、块信息
        @Test
        public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
            // 1获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 获取文件详情
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

            while(listFiles.hasNext()){
                LocatedFileStatus status = listFiles.next();

                // 输出详情
                // 文件名称
                System.out.println(status.getPath().getName());
                // 长度
                System.out.println(status.getLen());
                // 权限
                System.out.println(status.getPermission());
                // z组
                System.out.println(status.getGroup());

                // 获取存储的块信息
                BlockLocation[] blockLocations = status.getBlockLocations();

                for (BlockLocation blockLocation : blockLocations) {

                    // 获取块存储的主机节点
                    String[] hosts = blockLocation.getHosts();

                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }

                System.out.println("----------------数据分割线-----------");
            }
        }


        //文件与文件夹判断

        @Test
        public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

            // 1 获取文件配置信息
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 判断是文件还是文件夹
            FileStatus[] listStatus = fs.listStatus(new Path("/"));

            for (FileStatus fileStatus : listStatus) {

                // 如果是文件
                if (fileStatus.isFile()) {
                    System.out.println("f:"+fileStatus.getPath().getName());
                }else {
                    System.out.println("d:"+fileStatus.getPath().getName());
                }
            }

            // 3 关闭资源
            fs.close();
        }




        //------------------------IO流方式操作

        //文件上传
        @Test
        public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 创建输入流
            FileInputStream fis = new FileInputStream(new File("/Users/chenjunying/Downloads/1.txt"));

            // 3 获取输出流
            FSDataOutputStream fos = fs.create(new Path("/test2/2.txt")); //需要指定文件名称及路径

            // 4 流对拷
            IOUtils.copyBytes(fis, fos, configuration);

            // 5 关闭资源
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }


        // 文件下载
        @Test
        public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 获取输人流
            FSDataInputStream fis = fs.open(new Path("/test2/2.txt"));

            // 3 获取输出流
            FileOutputStream fos = new FileOutputStream(new File("/Users/chenjunying/Downloads/3.txt"));

            // 4 流的对拷
            IOUtils.copyBytes(fis, fos, configuration);

            // 5 关闭资源
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
            fs.close();
        }

        //---------------------下载文件过大，可以定位下载

        //下载第一块
        @Test
        public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 获取输入流
            FSDataInputStream fis = fs.open(new Path("/test2/hadoop-2.7.2.tar.gz"));

            // 3 创建输出流
            FileOutputStream fos = new FileOutputStream(new File("/Users/chenjunying/Downloads/hadoop-2.7.2.tar.gz.part1"));

            // 4 流的拷贝
            byte[] buf = new byte[1024];

            for(int i =0 ; i < 1024 * 128; i++){
                fis.read(buf);
                fos.write(buf);
            }

            // 5关闭资源
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }

        //下载第二块数据
        @Test
        public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            // 2 打开输入流
            FSDataInputStream fis = fs.open(new Path("/test2/hadoop-2.7.2.tar.gz"));

            // 3 定位输入数据位置
            fis.seek(1024*1024*128);

            // 4 创建输出流
            FileOutputStream fos = new FileOutputStream(new File("/Users/chenjunying/Downloads/hadoop-2.7.2.tar.gz.part2"));

            // 5 流的对拷
            IOUtils.copyBytes(fis, fos, configuration);

            // 6 关闭资源
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }

        //合并文件
        /**
         * 1。在window命令窗口中执行
         * type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
         * 2。linux下分割/合并文件 https://blog.csdn.net/wq3028/article/details/80775626
         * 3。mack下：https://blog.csdn.net/qq_45036710/article/details/100163689
         */
}
