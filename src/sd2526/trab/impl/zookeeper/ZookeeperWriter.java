package sd2526.trab.impl.zookeeper;

import java.util.Scanner;

import org.apache.zookeeper.CreateMode;

public class ZookeeperWriter {
	// Main just for testing purposes
	public static void main(String[] args) throws Exception {

		Scanner sc = new Scanner(System.in);

		System.out.println("Provide a path (should start with /) :");
		String path = sc.nextLine().trim();

		System.out.println("Provide a value :");
		String value = sc.nextLine().trim();
		sc.close();

		ZookeeperProcessor zk = new ZookeeperProcessor("localhost:2181,kafka:2181");
		zk.write(path, null, CreateMode.PERSISTENT);

		String newPath = zk.write(path + "/node_", value, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("create new znode: " + newPath);
	}

}