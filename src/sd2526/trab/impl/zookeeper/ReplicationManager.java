package sd2526.trab.impl.zookeeper;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.hsqldb.persist.Log;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import sd2526.trab.impl.db.utils.JSON;
import sd2526.trab.impl.java.utils.ReplicationLogEntry;
import java.nio.file.Files;
import java.net.URI;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import java.io.FileWriter;

public class ReplicationManager {

  private static final Logger Log = Logger.getLogger(ReplicationManager.class.getName());

  // A versão atual do estado deste servidor (começaste muito bem aqui!)
  private final AtomicLong cur_version = new AtomicLong(0L);
  private final ExecutorService executor = Executors.newCachedThreadPool();
  // Referência para a eleição de líder (para saber o estado atual do nó)
  private final LeaderElection leaderElection;

  public ReplicationManager(LeaderElection leaderElection) {
    this.leaderElection = leaderElection;
  }

  /**
   * Verifica se este servidor é atualmente o primário.
   * Útil para os endpoints REST saberem se podem aceitar operações de escrita.
   */
  public boolean isPrimary() {
    return leaderElection.isLeader();
  }

  /**
   * Usado no Primário: Após processar e propagar uma escrita com sucesso,
   * incrementa a versão local.
   */
  public long incrementAndGetVersion() {
    return cur_version.incrementAndGet();
  }

  /**
   * Usado no Secundário: Atualiza a versão local após receber e aplicar
   * uma atualização enviada pelo Primário.
   */
  public void updateVersion(long newVersion) {
    cur_version.accumulateAndGet(newVersion, Math::max);
  }

  /**
   * Retorna a versão atual.
   * O servidor REST vai precisar disto para injetar no cabeçalho X-MESSAGES nas
   * respostas.
   */
  public long getCurrentVersion() {
    return cur_version.get();
  }

  /**
   * Garante a semântica "Read Your Writes" / "Monotonic Reads".
   * Se o cliente enviar um cabeçalho X-MESSAGES com valor X, o servidor tem de
   * bloquear
   * a leitura até que a sua versão local seja >= X.
   */
  public void awaitVersion(long targetVersion) {
    while (cur_version.get() < targetVersion) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        Log.warning("Interrupted while waiting for version " + targetVersion);
      }
    }
  }

  /**
   * Retorna o URI do Primário atual.
   * Usado pelas réplicas secundárias para reencaminhar pedidos (Forwarding).
   */
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
      // Só adiciona se NÃO FOR o URI do próprio Primário
      if (!uri.equals(myPrimaryUri)) {
        secondaries.add(uri);
      }
    }
    return secondaries;
  }

  private void saveToLocalDisk(ReplicationLogEntry entry) {
    try {
      String jsonLine = JSON.encode(entry) + "\n";

      String fileName = "operations_log_" + cur_version.get() + ".txt"; // Podes adaptar o nome

      Files.writeString(
          Paths.get("operations_log.txt"),
          jsonLine,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (Exception e) {
      Log.severe("Erro fatal ao escrever no disco local: " + e.getMessage());
    }
  }

  public void logAndPropagateToSecundaries(ReplicationLogEntry wrapper) {
    saveToLocalDisk(wrapper);

    List<String> uris = getSecundariesUris();
    if (uris.isEmpty())
      return;

    Client client = ClientBuilder.newClient();

    for (String uri : uris) {

      executor.submit(() -> {
        try {
          String endpoint = uri + "/internal/replicate";

          client.target(endpoint)
              .request()
              .post(Entity.entity(wrapper, MediaType.APPLICATION_JSON));

          Log.info("Replicado com sucesso para: " + uri);
        } catch (Exception e) {
          Log.warning("Falha a replicar para " + uri);
        }
      });

    }
  }

}