package sd2526.trab.impl.zookeeper;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class ReplicationManager {

  private static final Logger Log = Logger.getLogger(ReplicationManager.class.getName());

  // A versão atual do estado deste servidor (começaste muito bem aqui!)
  private final AtomicLong cur_version = new AtomicLong(0L);

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
}