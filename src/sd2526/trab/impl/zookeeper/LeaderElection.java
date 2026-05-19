package sd2526.trab.impl.zookeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher {
	private static Logger Log = Logger.getLogger(LeaderElection.class.getName());

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
		System.setProperty("java.util.logging.level", "INFO");
	}

	private String zookeeperAddress;
	private String myZNode;
	private String leader;
	private String leaderURI; // Substitui o leaderHost
	private String myURI; // Substitui o myHost
	private String zDir;
	private ZooKeeper zk;

	private LeadershipChangeListener listener;

	public interface LeadershipChangeListener {
		void onLeadershipChanged(boolean isLeader, String leaderHost);
	}

	public LeaderElection(String zooAddress, String serviceName, String myURI) {
		this.zookeeperAddress = zooAddress;
		this.zDir = "/" + serviceName;
		this.myURI = myURI;
		this.leader = "";
		this.myZNode = "";
	}

	public void setLeadershipChangeListener(LeadershipChangeListener listener) {
		this.listener = listener;
	}

	public boolean isLeader() {
		return myZNode != null && myZNode.equals(leader);
	}

	public String getLeaderURI() {
		return leaderURI;
	}

	public void start() {
		try {
			zk = new ZooKeeper(zookeeperAddress, 3000, this);
			try {
				zk.create(zDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException.NodeExistsException e) {
			}

			// Aqui gravamos o URI COMPLETO em vez de myHost
			String fullZNodePath = zk.create(zDir + "/node_", myURI.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);

			this.myZNode = fullZNodePath.substring(fullZNodePath.lastIndexOf("/") + 1);
			Log.info("Created my personal znode: " + myZNode);

			zk.getChildren(zDir, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			try {
				List<String> children = zk.getChildren(zDir, true);
				this.checkLeadership(children);
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void checkLeadership(List<String> znodes) {
		try {
			if (znodes.isEmpty())
				return;

			Collections.sort(znodes);
			String newLeader = znodes.get(0);

			byte[] data = zk.getData(zDir + "/" + newLeader, false, null);
			this.leaderURI = new String(data); // Agora recupera o URI completo!

			this.leader = newLeader;
			boolean isNowLeader = this.isLeader();

			Log.info("Leader = '" + leader + "' at URI " + leaderURI + " ; my znode = " + myZNode
					+ " ; I am the leader = " + isNowLeader);

			if (listener != null) {
				listener.onLeadershipChanged(isNowLeader, leaderURI);
			}
		} catch (Exception e) {
			Log.severe("Error checking leadership: " + e.getMessage());
		}
	}
}