package com.cjy.lock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.cjy.zk.lock.ChildrenBlockingLock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 测试利用子节点实现的阻塞锁
 *
 * @author code story
 * @date 2019/8/19
 */
public class ChildrenBlockingLockTest  {
    protected final String address = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    protected Logger log = LoggerFactory.getLogger(getClass());


    /**
     * 测试单线程，添加删除是否正常
     */
    @Test
    public void testChildrenBlocking() throws IOException {
//        String guidNodeName = "/guid-" + System.currentTimeMillis();
        String guidNodeName = "/guid-";
        String clientGuid = "client-0";

        ChildrenBlockingLock zooKeeperLock = new ChildrenBlockingLock(address);

        boolean assertResult = zooKeeperLock.exists(guidNodeName) == false;
        log.info("锁还未生成，不存在。");
        assert assertResult;

        assertResult = zooKeeperLock.lock(guidNodeName, clientGuid);
        log.info("获取分布式锁应该成功。");
        assert assertResult;

        assertResult = zooKeeperLock.exists(guidNodeName) == true;
        log.info("锁已经生成。");
        assert assertResult;

        assertResult = zooKeeperLock.release(guidNodeName, clientGuid) == true;
        log.info("正常释放锁，应该成功。");
        assert assertResult;

        assertResult = zooKeeperLock.exists(guidNodeName) == false;
        log.info("锁已被删除，应该不存在。");
        assert assertResult;

    }


    /**
     * 测试多线程，添加删除是否正常
     */
    @Test
    public void testChildrenBlockingMultiThread() throws IOException, InterruptedException {
//        String guidNodeName = "/multi-" + System.currentTimeMillis();
        String guidNodeName = "/multi-";
        //
        int threadCount = LockClientThread.threadCount;
        //初始化现场计数器
        LockClientThread.successLockSemaphore = new CountDownLatch(threadCount);

        LockClientThread[] threads = new LockClientThread[threadCount];
        //初始化线程
        for (int i = 0; i < threadCount; i++) {
            ChildrenBlockingLock nodeBlocklessLock = new ChildrenBlockingLock(address);
            threads[i] = new LockClientThread(nodeBlocklessLock, guidNodeName, "client-" + (i + 1));
        }
        //开启线程
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        //子线程跑完，再结束main线程
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        assert LockClientThread.successLockSemaphore.getCount() == 0;

        /**节点变化过程
         * [zk: localhost:2181(CONNECTED) 31] ls /multi-
         * [element0000000008, element0000000009, element0000000006, element0000000007, element0000000005]
         * [zk: localhost:2181(CONNECTED) 32] ls /multi-
         * [element0000000008, element0000000009, element0000000006, element0000000007]
         * [zk: localhost:2181(CONNECTED) 33] ls /multi-
         * [element0000000008, element0000000009, element0000000007]
         * [zk: localhost:2181(CONNECTED) 34] ls /multi-
         * [element0000000008, element0000000009]
         * [zk: localhost:2181(CONNECTED) 36] ls /multi-
         * [element0000000009]
         * [zk: localhost:2181(CONNECTED) 37] ls /multi-
         * []
         * [zk: localhost:2181(CONNECTED) 38]
         */
    }
}