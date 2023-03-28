import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;

public class ZookeeperOperationTest {

    private ZooKeeper zooKeeper;

    @Before
    public void setup() throws IOException {
        String connectString = "cluster1:2181,cluster2:2181,cluster3:2181";
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(MessageFormat.format("zookeeper watcher event {0}", event));
            }
        };

        this.zooKeeper = new ZooKeeper(connectString, 2000, watcher);
    }

    @After
    public void uninstall() throws InterruptedException {
        this.zooKeeper.close();
    }

    @Test
    public void createZookeeperInstanceTest() throws IOException, InterruptedException {
        // 等待 zookeeper 连接成功
        Thread.sleep(5 * 1000);
        Assert.assertNotNull(zooKeeper);
        System.out.println(MessageFormat.format("zookeeper create success: {0}", zooKeeper));
    }

    @Test
    public void createZnodeTest() throws InterruptedException, KeeperException {
        String path = "/demo_znode";
        byte[] data = "znode data".getBytes(StandardCharsets.UTF_8);
        ArrayList<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        CreateMode createMode = CreateMode.PERSISTENT;

        String returnPath = zooKeeper.create(path, data, acl, createMode);

        System.out.println(MessageFormat.format("zookeeper create znode {0}", returnPath));
    }

    @Test
    public void getZnodeTest() throws InterruptedException, KeeperException {
        String path = "/demo_znode";

        byte[] data = zooKeeper.getData(path, false, null);

        System.out.println(MessageFormat.format("zookeeper get znode \"{0}\" value \"{1}\"", path, new String(data)));
    }

    @Test
    public void setZnodeTest() throws InterruptedException, KeeperException {
        String path = "/demo_znode";
        byte[] data = "znode data next version.".getBytes(StandardCharsets.UTF_8);

        Stat stat = zooKeeper.setData(path, data, -1);

        System.out.println(MessageFormat.format("zookeeper set znode data stat {0}", stat));
    }

    @Test
    public void deleteZnodeTest() throws InterruptedException, KeeperException {
        String path = "/demo_znode";

        zooKeeper.delete(path, -1);

        System.out.println(MessageFormat.format("zookeeper delete znode {0}", path));
    }

    @Test
    public void existsZnodeTest() throws InterruptedException, KeeperException {
        String path = "/demo_znode";

        Stat stat = zooKeeper.exists(path, false);

        System.out.println(MessageFormat.format("zookeeper exists znode stat {0}", stat));
    }

}
