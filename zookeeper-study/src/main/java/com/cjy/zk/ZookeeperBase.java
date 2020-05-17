package com.cjy.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperBase implements Watcher {

    //日志对象
    protected Logger log = null;
    //客户端对象
    private ZooKeeper zkClient = null;
    //等待连接建立成功的信号
    private CountDownLatch countDownLactch = new CountDownLatch(1);

    //收到的所有Event
    List<WatchedEvent> watchedEventList = new ArrayList<>();
    //避免重复节点
    static Integer rootNodeInitial = Integer.valueOf(1);

    public ZookeeperBase(String address) throws IOException {

        this(address,30000);

    }

    /**
     * 创建zk客户端
     * @param address 地址
     * @param sessionTimeout 超时设置
     * @throws IOException
     */
    public ZookeeperBase(String address,int sessionTimeout) throws IOException {

        //初始化日志对象
        log = LoggerFactory.getLogger(this.getClass());
        //初始化客户端对象
        zkClient = new ZooKeeper(address,sessionTimeout,this);

        try {
            //等待客户端成功连接zookeeper服务器才继续往下执行
            countDownLactch.await();
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }
    /**
     *
     * 监听器回调处理函数
     * @param watchedEvent
     */
    @Override
    public final void process(WatchedEvent watchedEvent) {

        //判断事件类型
        if(Event.EventType.None.equals(watchedEvent.getType())){
            log.info("--------服务端连接事件");
            //连接建立成功
            if (Event.KeeperState.SyncConnected.equals(watchedEvent.getState())){
                log.info("--------服务端连接成功");
                countDownLactch.countDown();
            }
        }else {
            watchedEventList.add(watchedEvent);
            if(Event.EventType.NodeCreated.equals(watchedEvent.getType())){
                log.info("--------节点新增事件");
                processNodeCreated(watchedEvent);
            }else if(Event.EventType.NodeDeleted.equals(watchedEvent.getType())){
                log.info("--------节点删除事件");
                processNodeDeleted(watchedEvent);
            }else if(Event.EventType.NodeDataChanged.equals(watchedEvent.getType())){
                log.info("--------节点数据修改事件");
                processNodeDataChanged(watchedEvent);

            }else if(Event.EventType.NodeChildrenChanged.equals(watchedEvent.getType())){
                log.info("--------子节点修改事件");
                processNodeChildrenChanged(watchedEvent);


           /*     //为了测试子节点下的数据持续变化，二次开启监听
                try {
                    List<String> children = zkClient.getChildren("/test", true);
                    System.out.println(children);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
        }

    }


    /**
     * 处理事件：节点新增
     * @param watchedEvent
     */
    protected void processNodeCreated(WatchedEvent watchedEvent){

    }

    /**
     * 处理事件：节点删除
     * @param watchedEvent
     */
    protected void processNodeDeleted(WatchedEvent watchedEvent) {
    }

    /**
     * 处理事件：节点数据修改
     * @param watchedEvent
     */
    protected void processNodeDataChanged(WatchedEvent watchedEvent) {
    }

    /**
     * 处理事件：子节点变化事件
     * @param watchedEvent
     */

    protected void processNodeChildrenChanged(WatchedEvent watchedEvent) {
    }

    /**
     * 创建测试需要的根节点--持久类型
     *
     * @param rootNodeName
     * @return
     */
    public String createRootNode(String rootNodeName) {
        CreateMode createMode = CreateMode.PERSISTENT;
        return createRootNode(rootNodeName, createMode);
    }


    /**
     * 创建测试需要的根节点，需要指定 CreateMode
     *
     * @param rootNodeName
     * @param createMode
     * @return
     */
    public String createRootNode(String rootNodeName, CreateMode createMode) {

        synchronized (rootNodeInitial) {
            // 创建 tableSerial 的zNode
            try {
                Stat existsStat = getZkClient().exists(rootNodeName, false);
                if (existsStat == null) {
                    rootNodeName = getZkClient().create(rootNodeName, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
                }
            } catch (KeeperException e) {
                log.info("创建节点失败，可能是其他客户端已经创建", e);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
        }
        return rootNodeName;
    }
    //获取客户度对象
    public ZooKeeper getZkClient(){
        return zkClient;
    }

    public List<WatchedEvent> getWatchedEventList(){
        return watchedEventList;
    }


}
