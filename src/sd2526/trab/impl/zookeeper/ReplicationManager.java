package sd2526.trab.impl.zookeeper;

import java.util.concurrent.atomic.AtomicLong;

import sd2526.trab.impl.zookeeper.*;

public class ReplicationManager {

  final AtomicLong cur_version = new AtomicLong(0L);
}
