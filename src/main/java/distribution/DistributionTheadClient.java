package distribution;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

public class DistributionTheadClient {

    private ZooKeeper zooKeeper;
    private String parentPath;
    private String subPath;

    private volatile String thisPath;

    public DistributionTheadClient(String hostPorts, String parentPath, String subPath) throws IOException, InterruptedException, KeeperException {
        this.zooKeeper = new ZooKeeper(hostPorts, 5 * 1000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (Event.EventType.NodeChildrenChanged.equals(event.getType())) {
                    try {
                        List<String> children = zooKeeper.getChildren(parentPath, true);

                        Collections.sort(children);
                        int i = children.indexOf(thisPath.substring((parentPath + "/").length()));
                        if (0 == i) {
                            doSomething();
                            thisPath = zooKeeper.create(parentPath + "/" + subPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        });

        this.parentPath = parentPath;
        this.subPath = subPath;

        createIfNotExists(parentPath);

        this.thisPath = zooKeeper.create(parentPath + "/" + subPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> children = zooKeeper.getChildren(this.parentPath, true);
        if (children.size() == 1) {
            this.doSomething();

            this.thisPath = zooKeeper.create(parentPath + "/" + subPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

    }

    private void doSomething() throws InterruptedException, KeeperException {
        try {
            System.out.println(MessageFormat.format("thread {0} starting doing something with path {1} ", Thread.currentThread().getName(), this.thisPath));

            long millis = Math.abs(new Random().nextInt(10)) * 1000L;
            Thread.sleep(millis);
        } finally {
            System.out.println(MessageFormat.format("thread {0} finished does something with path {1} ", Thread.currentThread().getName(), this.thisPath));
            zooKeeper.delete(this.thisPath, -1);
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
        String hostPorts = "cluster1:2181,cluster2:2181,cluster3:2181";
        String parentPath = "/distribution_thread";
        String client1 = "client";

        IntStream.range(0, 10).forEach(i -> {
            try {
                new DistributionTheadClient(hostPorts, parentPath, client1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread.sleep(Integer.MAX_VALUE);
    }

}
