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
		// summarizes the logging format
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
		System.setProperty("java.util.logging.level", "ERROR");
	}

	private String zookeeperAddress;
	private String myZNode;
	private String leader;
	private String myHost;
	private String zDir;
	private ZooKeeper zk;

	/**
	 * @param zooAddress  a string in the format host:port that allows to contact
	 *                    the zookeeper service
	 * @param serviceName the name of the service for which we are conducting leader
	 *                    election
	 */
	LeaderElection(String zooAddress, String serviceName) {
		this.zookeeperAddress = zooAddress;
		zDir = "/" + serviceName;

		try {
			myHost = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			myHost = "localhost";
		}

		leader = "leader";
		myZNode = "";
	}

	public void start() {
		try {
			zk = new ZooKeeper(zookeeperAddress, 3000, this);

			try {
				// Try to create the main znode for the service (might already exist)
				zk.create(zDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException | InterruptedException e) {
				Log.info("Cloud not create the zkNode: " + zDir + "(" + e.getMessage() + ")");
			}

			// Create the ephemeral znode for the host itself
			myZNode = zk.create(zDir + "/host_", myHost.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);

			Log.info("Created my personal znode: " + myZNode);
			
			myZNode = myZNode.substring(myZNode.lastIndexOf("/") + 1);

			//The watch will trigger immediatly because there is at least one node below the root zknode
			zk.getChildren(zDir, true); 
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			List<String> children = zk.getChildren(zDir, true);
			this.checkLeadership(children);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void checkLeadership(List<String> znodes) {
		try {
			Collections.sort(znodes);
			this.leader = znodes.get(0);

			Log.info("Leader znode: " + this.leader);
			
			byte[] data = zk.getData(zDir + "/" + this.leader, false, null);
			String leaderHost = new String(data);

			Log.info("Leader = '" + leader + "' running on host " + leaderHost + " ; my znode = " + myZNode
					+ " ; I am the leader = " + myZNode.equals(leader));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// Main just for testing purposes
	public static void main(String[] args) throws Exception {
		LeaderElection election = new LeaderElection("localhost:2181", "test");
		election.start();

		new Thread(() -> {
			for (;;) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					// do nothing
				}
			}
		}).start();

	}
}