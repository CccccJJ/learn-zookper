package distribution;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

public class DistributionClient {


    private String parentNode;
    private String createNode;
    private String waitNode;
    private ZooKeeper zooKeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private Watcher defaultWatcher = event -> {
        // 连接成功
        if (Watcher.Event.KeeperState.SyncConnected.equals(event.getState())) {
            countDownLatch.countDown();
        }

        // 等待的节点已经被删除
        if (Watcher.Event.EventType.NodeDeleted.equals(event.getType())
                && event.getPath().equals(parentNode + "/" + waitNode)) {
            try {
                handle();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    private DistributionClient(String hostPorts, String parentNode) throws IOException, InterruptedException, KeeperException {
        this.parentNode = parentNode;
        this.zooKeeper = new ZooKeeper(hostPorts, 5 * 1000, defaultWatcher);
        countDownLatch.await();

        createIfNotExists(parentNode);

        this.createNode = zooKeeper.create(parentNode + "/" + getClass().getSimpleName(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(MessageFormat.format("thread {0} created path {1} success.", Thread.currentThread().getName(), createNode));

        List<String> children = zooKeeper.getChildren(parentNode, false);
        if (children.size() == 1) {
            handle();
        } else {
            Collections.sort(children);

            String node = this.createNode.replace(parentNode + "/", "");
            int i = children.indexOf(node);
            if (i == -1) {
                // never happen;
                System.exit(2);
            } else if (i == 0) {
                handle();
            } else {
                this.waitNode = children.get(i - 1);
                zooKeeper.getData(parentNode + "/" + waitNode, true, new Stat());
            }
        }
    }

    public void handle() throws InterruptedException, KeeperException {
        try {
            System.out.println(MessageFormat.format("handling node {0} with thread {1}", this.createNode, Thread.currentThread().getName()));
            Thread.sleep(3 * 1000);
        } finally {
            zooKeeper.delete(createNode, -1);
            System.out.println(MessageFormat.format("delete node {0}", this.createNode));
        }

    }

    private void createIfNotExists(String path) throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists(path, false);
        if (Objects.nonNull(exists)) {
            return;
        }

        zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static void main(String[] args) throws InterruptedException {
        final String hostPorts = "cluster1:2181,cluster2:2181,cluster3:2181";
        final String parentNode = "/distribution_client";

        IntStream.range(0, 3).forEachOrdered(i -> new Thread(() -> {
            try {
                new DistributionClient(hostPorts, parentNode);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start());

        Thread.sleep(30 * 1000);

        System.out.println("=============end===============");
    }

}
