package sd2526.trab.impl.zookeeper;

import java.util.List;
import java.util.Scanner;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZookeeperGetChildren {

	// Main just for testing purposes
	public static void main(String[] args) throws Exception {
		Scanner sc = new Scanner(System.in);

		System.out.println("Provide a path (should start with /) :");
		String path = sc.nextLine().trim();

		final ZookeeperProcessor zk = new ZookeeperProcessor("localhost:2181,kafka:2181");
		zk.getChildren(path, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				List<String> lst = zk.getChildren(path, this);
				for (String str : lst)
					System.out.println(str);
				System.out.println();
			}

		});

		System.out.println("Enter for stop observing changes :");
		sc.nextLine().trim();

		sc.close();
	}

}