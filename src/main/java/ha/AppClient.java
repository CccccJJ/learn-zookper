package ha;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AppClient {

    private List<String> serverAddress;

    private String watchPath;
    private ZooKeeper zooKeeper;
    private Watcher watcher = event -> {
        Watcher.Event.EventType type = event.getType();
        final String path = event.getPath();

        if (Watcher.Event.EventType.None.equals(type)) {
            try {
                queryNodeList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 路径为目标监听路径 且 子路径发生了变化
        if (watchPath.equals(path)
                || Watcher.Event.EventType.NodeChildrenChanged.equals(type)) {

            try {
                queryNodeList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public AppClient(String hostPorts, String watchPath) throws IOException {
        this.watchPath = watchPath;
        zooKeeper = new ZooKeeper(hostPorts, 5 * 1000, watcher);
    }

    public AppClient(String hostsPorts, String watchPath, Watcher watcher) throws IOException {
        this.watchPath = watchPath;
        zooKeeper = new ZooKeeper(hostsPorts, 5 * 1000, watcher);
    }

    private void queryNodeList() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(watchPath, true);

        this.serverAddress = children.stream().map(child -> {
            String childData = null;
            try {
                byte[] data = zooKeeper.getData(watchPath + "/" + child, false, null);
                childData = new String(data);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return childData;
        }).filter(Objects::nonNull).collect(Collectors.toList());

        System.out.println(MessageFormat.format("children has been changed {0}", this.serverAddress));
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        String hostPorts = "cluster1:2181,cluster2:2181,cluster3:2181";
        String watchPath = "/ha";

        AppClient appClient = new AppClient(hostPorts, watchPath);

        Thread.sleep(Integer.MAX_VALUE);
    }

}
