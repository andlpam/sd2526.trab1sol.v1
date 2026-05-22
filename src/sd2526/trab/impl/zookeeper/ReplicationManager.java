package sd2526.trab.impl.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.api.rest.RestReplicatedMessages;
import sd2526.trab.impl.db.utils.JSON;
import sd2526.trab.impl.java.utils.ReplicationLogEntry;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationManager {

  private static final Logger Log = Logger.getLogger(ReplicationManager.class.getName());

  // Estado e Locks
  private final AtomicLong cur_version = new AtomicLong(0L);
  private final Object versionLock = new Object(); // Lock para eficiência no Monotonic Reads
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final java.util.Map<String, ReplicationDispatcher> dispatchers = new ConcurrentHashMap<>();

  private final LeaderElection leaderElection;
  private final String secret; // A chave partilhada Server-to-Server

  // Construtor atualizado para receber o secret
  public ReplicationManager(LeaderElection leaderElection, String secret) {
    this.leaderElection = leaderElection;
    this.secret = secret;
  }

  public String getSecret() {
    return this.secret;
  }

  public boolean isPrimary() {
    return leaderElection.isLeader();
  }

  public long incrementAndGetVersion() {
    synchronized (versionLock) {
      long v = cur_version.incrementAndGet();
      versionLock.notifyAll(); // Acorda possíveis leituras locais
      return v;
    }
  }

  public void updateVersion(long newVersion) {
    synchronized (versionLock) {
      cur_version.accumulateAndGet(newVersion, Math::max);
      versionLock.notifyAll(); // Acorda os clientes à espera do Monotonic Reads
    }
  }

  public long getCurrentVersion() {
    return cur_version.get();
  }

  public long getLocalVersion() {
    return cur_version.get();
  }

  public void setLocalVersion(long version) {
    updateVersion(version);
  }

  /**
   * Otimizado: Em vez de Thread.sleep (que desperdiça CPU e adiciona latência),
   * usa o wait() e notifyAll() do Java para acordar instantaneamente.
   */
  public void awaitVersion(long targetVersion) {
    synchronized (versionLock) {
      while (cur_version.get() < targetVersion) {
        try {
          versionLock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          Log.warning("Interrupted while waiting for version " + targetVersion);
        }
      }
    }
  }

  public String getPrimaryURI() {
    if (leaderElection != null) {
      return leaderElection.getLeaderURI();
    }
    return null;
  }

  private List<String> getSecundariesUris() {
    List<String> allUris = leaderElection.getAllNodesURIs();
    List<String> secondaries = new ArrayList<>();
    String myPrimaryUri = leaderElection.getLeaderURI();

    for (String uri : allUris) {
      if (!uri.equals(myPrimaryUri)) {
        secondaries.add(uri);
      }
    }
    return secondaries;
  }

  public void saveToLocalDisk(ReplicationLogEntry entry) {
    try {
      String jsonLine = JSON.encode(entry) + "\n";
      // Guarda em disco
      Files.writeString(
          Paths.get("operations_log.txt"),
          jsonLine,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (Exception e) {
      Log.severe("Erro fatal ao escrever no disco local: " + e.getMessage());
    }
  }

  // Substituis o teu logAndPropagateToSecundaries por este:
  public void logAndPropagateToSecundaries(ReplicationLogEntry wrapper) {
    saveToLocalDisk(wrapper); // Escreve o log em disco primeiro

    List<String> uris = getSecundariesUris();

    for (String uri : uris) {
      // Se ainda não temos um dispatcher para este secundário, criamos um
      dispatchers.computeIfAbsent(uri, k -> new ReplicationDispatcher(uri, secret));

      // Entregamos a operação à fila do dispatcher (retorna instantaneamente)
      dispatchers.get(uri).dispatch(wrapper);
    }
  }
}