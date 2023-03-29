package ha;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class AppServer {

    private ZooKeeper zooKeeper;
    private String parentNodePath;
    private CountDownLatch latch = new CountDownLatch(1);

    public AppServer(String hostPorts, String path, String address) throws IOException, InterruptedException, KeeperException {
        this.parentNodePath = path;

        this.zooKeeper = new ZooKeeper(hostPorts, 5 * 1000, event -> {
            // 连接成功
            if (Watcher.Event.EventType.None.equals(event.getType())) {
                latch.countDown();
            }
        });

        latch.await();

        createIfNotExists(this.parentNodePath);

        // 创建znode（临时、序列）
        String createdPath = zooKeeper.create(parentNodePath + "/" + getClass().getSimpleName(), address.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(MessageFormat.format("app server 创建节点 {0} 内容 {1}", createdPath, address));
    }

    private void createIfNotExists(String path) throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists(path, false);
        if (Objects.nonNull(exists)) {
            return;
        }

        zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String address = args[0];
        String hostPorts = "cluster1:2181,cluster2:2181,cluster3:2181";
        String parentPath = "/ha";

        AppServer appServer = new AppServer(hostPorts, parentPath, address);
        System.out.println(MessageFormat.format("app server created successes, address is {0}", address));

        Thread.sleep(Integer.MAX_VALUE);
    }

}
