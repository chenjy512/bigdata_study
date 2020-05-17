package com.cjy.watcher;

import com.cjy.zk.ZookeeperBase;
import com.cjy.zk.constants.Constants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatcherTypeTest {


    private String url = Constants.URL;
    private ZookeeperBase zookeeperBase;
    ZooKeeper zkClient = null;

    {
        try {
            zookeeperBase = new ZookeeperBase(url);
            zkClient = zookeeperBase.getZkClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        WatcherTypeTest ts = new WatcherTypeTest();
//        ts.createNode();
//        ts.changeNodeData();
//        ts.deleteNode();

        /**以上三个函数的监听变化
         * 2020-05-17 21:02:29,069 INFO [com.cjy.zk.ZookeeperBase] - --------服务端连接事件
         * 2020-05-17 21:02:29,070 INFO [com.cjy.zk.ZookeeperBase] - --------服务端连接成功
         * 创建节点 test
         * 2020-05-17 21:02:29,085 INFO [com.cjy.zk.ZookeeperBase] - --------节点新增事件
         * /test
         * 2020-05-17 21:02:29,090 INFO [com.cjy.zk.ZookeeperBase] - --------节点数据修改事件
         * 2020-05-17 21:02:29,094 INFO [com.cjy.zk.ZookeeperBase] - --------节点删除事件
         */
        ts.childrenNodeChange();
    }

    //节点新增
    public void createNode() throws KeeperException, InterruptedException {
        zkClient.exists("/test",true);
        System.out.println("创建节点 test");
        String s = zkClient.create("/test", "接节点新增".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    //节点数据修改
    public void changeNodeData() throws KeeperException, InterruptedException {
        zkClient.exists("/test",true);
        Stat stat = zkClient.setData("/test", "修改节点数据".getBytes(), -1);
    }

    //节点删除
    public void deleteNode() throws KeeperException, InterruptedException {
        zkClient.exists("/test",true);
        zkClient.delete("/test",-1);
    }
    //子节点监控

    public void childrenNodeChange() throws KeeperException, InterruptedException {
        //获取test下的所有子节点并监控
        List<String> children = zkClient.getChildren("/test", true);
        System.out.println(children);
        Thread.sleep(Long.MAX_VALUE);
    }
}
