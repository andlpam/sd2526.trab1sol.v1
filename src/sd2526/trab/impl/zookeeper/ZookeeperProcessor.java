package sd2526.trab.impl.zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperProcessor implements Watcher {
	
	static {
		// summarizes the logging format
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
		System.setProperty("java.util.logging.level", "ERROR");
	}
	
	private ZooKeeper zk;

	/**
	 * @param  hostPort a string in the format host:port to connect to zookeeper
	 */
	ZookeeperProcessor( String hostPort) throws Exception {
		zk = new ZooKeeper(hostPort, 3000, this);

	}
	
	public String write( String path, String value, CreateMode mode) {
		try {
			return zk.create(path, (value == null ? null : value.getBytes()), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<String> getChildren( String path, Watcher watch) {
		try {
			return zk.getChildren(path, watch);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	public List<String> getChildren( String path) {
		try {
			return zk.getChildren(path, false);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}
	
}