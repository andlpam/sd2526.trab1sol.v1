package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.db.Zoho;
import sd2526.trab.impl.java.servers.ProxyMessages;

public class RestProxyMessagesServer extends AbstractRestServer {

  private static Logger Log = Logger.getLogger(RestProxyMessagesServer.class.getName());
  public static final int PORT = 8082;

  public RestProxyMessagesServer(int port) {
    super(Log, Messages.SERVICE_NAME, port);
  }

  @Override
  protected void registerResources(ResourceConfig config) {
    config.register(new RestMessagesResource(ProxyMessages.getInstance()));
  }

  public static void main(String[] args) {
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

      ProxyMessages.getInstance();

      int port = args.length > 1 ? Integer.parseInt(args[1]) : PORT;

      new RestProxyMessagesServer(port).start();

    } catch (Exception e) {
      Log.severe("Erro crítico ao arrancar o servidor Zoho Proxy: " + e.getMessage());
      e.printStackTrace();
    }
  }
}