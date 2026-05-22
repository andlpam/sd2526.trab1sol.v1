package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.db.Zoho;
import sd2526.trab.impl.java.servers.ProxyMessages;

public class RestProxyMessagesServer extends AbstractRestServer {

  private static Logger Log = Logger.getLogger(RestProxyMessagesServer.class.getName());
  public static final int PORT = 8082;

  public RestProxyMessagesServer() {
    super(Log, Messages.SERVICE_NAME, PORT);
  }

  @Override
  protected void registerResources(ResourceConfig config) {
    config.registerInstances(new RestMessagesResource(ProxyMessages.getInstance()));
  }

  public static void main(String[] args) {
    System.out.println(">>>> MAIN DO PROXY A ARRANCAR! <<<<");
    try {
      boolean cleanState = false;
      if (args.length > 0) {
        cleanState = Boolean.parseBoolean(args[0]);
      }

      Log.info("A iniciar o RestProxyMessagesServer (Zoho). Clean State pedido: " + cleanState);

      if (cleanState) {
        Log.info("A limpar a caixa de entrada do Zoho para o Tester...");
        Zoho.getInstance().deleteAllEmails();
        Log.info("Caixa de entrada limpa com sucesso!");
      } else {
        Log.info("A manter o estado anterior (não foram apagados emails).");
      }

      new RestProxyMessagesServer().start();

    } catch (Throwable t) {
      System.out.println(">>>> ERRO CATASTRÓFICO <<<<");
      t.printStackTrace(System.out);
    }
  }
}