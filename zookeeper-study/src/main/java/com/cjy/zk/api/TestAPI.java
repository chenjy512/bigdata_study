package com.cjy.zk.api;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestAPI {

    //配置参数：url、连接事件
    private static String connectString ="10.211.55.102:2181,10.211.55.103:2181,10.211.55.104:2181";
    private static int sessionTimeout = 20000;
    private ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getType() + "--" + event.getPath() +"--"+event.getState().toString());
                // 再次启动监听
                try {
                    zkClient.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 创建子节点
    @Test
    public void create() throws Exception {
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型 PERSISTENT：永久无编号节点
        String nodeCreated = zkClient.create("/t1", "ccl".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //创建临时有序号节点
    @Test
    public void createTempNode() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/cjy/cs", "ooo".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("创建临时序号节点成功："+ nodeCreated);
        List<String> children = zkClient.getChildren("/cjy", false);
        for (String child : children) {
            System.out.println(child);
        }
    }

    // 获取子节点
    @Test
    public void getChildren() throws Exception {

        List<String> children = zkClient.getChildren("/", false);

        System.out.println(children);

        // 延时阻塞-查看监听变化，process中需要再次监听才能一直监听子节点
        Thread.sleep(Long.MAX_VALUE);
    }

    // 节点数据修改
    @Test
    public void set() throws KeeperException, InterruptedException {
        //-1 表示忽略所有版本号
        zkClient.setData("/t1","tt".getBytes(),-1);
    }

    //节点删除
    @Test
    public void delete() throws KeeperException, InterruptedException {
        //-1 表示忽略所有版本号
        zkClient.delete("/t1",-1);
    }




}
