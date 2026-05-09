package sd2526.trab.impl.db;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.impl.db.zoho.ZohoServiceFactory;
import sd2526.trab.impl.db.zoho.ZohoTokenManager;
import sd2526.trab.impl.db.zoho.msgs.ZohoAccount;
import sd2526.trab.impl.db.zoho.msgs.ZohoAccountReply;
import sd2526.trab.impl.db.zoho.msgs.ZohoEmailRequest;
import sd2526.trab.impl.db.zoho.msgs.ZohoMessageContentReply;
import sd2526.trab.impl.db.zoho.msgs.ZohoMessageListReply;
import sd2526.trab.impl.db.zoho.msgs.ZohoMessageMetadata;
import sd2526.trab.impl.db.utils.JSON;

public class Zoho {
    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

    static final String CLIENT_ID = "1000.CG5979N23BDQTJHC8SLB2Z16G4KKBS";
    static final String CLIENT_SECRET = "0a99a38deda6b9695a657bcba866d8fb9632aed531";
    static final String REFRESH_TOKEN = "1000.08f9088e27eff3ce246e4c6b046bcc7c.ffb2831c13f4098702cb1ce111b0ccb0";

    private static final String ACCOUNTS = "/accounts";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;

    // Cache da conta para não fazermos GET /accounts em todos os pedidos
    private String accountId = null;
    private String primaryEmail = null;

    private Zoho() {
        service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    synchronized public static Zoho getInstance() {
        if (instance == null)
            instance = new Zoho();
        return instance;
    }

    private void ensureAccountDetails() throws Exception {
        if (accountId == null || primaryEmail == null) {
            ZohoAccount acc = getAccount();
            if (acc != null) {
                this.accountId = acc.accountId();
                this.primaryEmail = acc.primaryEmailAddress();
            } else {
                throw new RuntimeException("Falha ao obter os detalhes da conta Zoho.");
            }
        }
    }

    public ZohoAccount getAccount() throws Exception {
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                var body = response.getBody();
                var data = JSON.decode(body, ZohoAccountReply.class).data();
                if (data == null || data.isEmpty())
                    return null;
                return data.get(0);
            } else {
                System.err.println(response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    // --- 1. Enviar Email (Guardar Mensagem) ---

    public void sendEmail(String subject, String body) throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        String url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages";
        OAuthRequest request = new OAuthRequest(Verb.POST, url);
        request.addHeader("Content-Type", "application/json");

        // Como a inbox serve apenas para nós guardarmos dados, enviamos de nós para nós
        // próprios
        var emailPayload = new ZohoEmailRequest(primaryEmail, primaryEmail, subject, body);
        request.setPayload(JSON.encode(emailPayload));

        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("Erro a enviar email: " + response.getBody());
            }
        }
    }

    // --- 2. Obter Todos os Emails (Ler Inbox) ---

    public List<String> getAllEmails() throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        List<String> emailBodies = new ArrayList<>();

        // Passo A: Obter a lista de mensagens (Metadata)
        String urlList = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages/view";
        OAuthRequest requestList = new OAuthRequest(Verb.GET, urlList);
        service.signRequest(accessToken, requestList);

        try (Response responseList = service.execute(requestList)) {
            if (responseList.isSuccessful()) {
                var listReply = JSON.decode(responseList.getBody(), ZohoMessageListReply.class);
                if (listReply.data() == null)
                    return emailBodies; // Inbox vazia

                // Passo B: Para cada mensagem, obter o conteúdo (Body)
                for (ZohoMessageMetadata meta : listReply.data()) {
                    String urlContent = MAIL_API_BASE + ACCOUNTS + "/" + accountId +
                            "/folders/" + meta.folderId() + "/messages/" + meta.messageId() + "/content";

                    OAuthRequest requestContent = new OAuthRequest(Verb.GET, urlContent);
                    service.signRequest(accessToken, requestContent);

                    try (Response responseContent = service.execute(requestContent)) {
                        if (responseContent.isSuccessful()) {
                            var contentReply = JSON.decode(responseContent.getBody(), ZohoMessageContentReply.class);
                            if (contentReply.data() != null) {
                                emailBodies.add(contentReply.data().content());
                            }
                        }
                    }
                }
            } else {
                System.err.println("Erro a listar emails: " + responseList.getBody());
            }
        }
        return emailBodies;
    }

    // --- 3. Apagar um Email Específico pelo 'mid' do teu Sistema ---

    public void deleteEmailBySystemMid(String targetMid) throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        // Passo A: Listar as mensagens
        String urlList = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages/view";
        OAuthRequest requestList = new OAuthRequest(Verb.GET, urlList);
        service.signRequest(accessToken, requestList);

        try (Response responseList = service.execute(requestList)) {
            if (!responseList.isSuccessful())
                return;
            var listReply = JSON.decode(responseList.getBody(), ZohoMessageListReply.class);
            if (listReply.data() == null)
                return;

            // Passo B: Procurar a mensagem correta iterando pelos conteúdos
            for (ZohoMessageMetadata meta : listReply.data()) {
                String urlContent = MAIL_API_BASE + ACCOUNTS + "/" + accountId +
                        "/folders/" + meta.folderId() + "/messages/" + meta.messageId() + "/content";

                OAuthRequest requestContent = new OAuthRequest(Verb.GET, urlContent);
                service.signRequest(accessToken, requestContent);

                try (Response responseContent = service.execute(requestContent)) {
                    if (responseContent.isSuccessful()) {
                        var contentReply = JSON.decode(responseContent.getBody(), ZohoMessageContentReply.class);
                        if (contentReply.data() != null && contentReply.data().content().contains(targetMid)) {

                            // Passo C: Se encontrou o 'mid' no corpo, apaga esta mensagem no Zoho
                            deleteMessagesInFolder(meta.folderId(), List.of(meta.messageId()), accessToken);
                            return; // Apagou, não precisa de continuar a procurar
                        }
                    }
                }
            }
        }
    }

    // --- 4. Apagar Todos os Emails (Limpar a conta para o Tester) ---

    public void deleteAllEmails() throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        String urlList = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages/view";
        OAuthRequest requestList = new OAuthRequest(Verb.GET, urlList);
        service.signRequest(accessToken, requestList);

        try (Response responseList = service.execute(requestList)) {
            if (!responseList.isSuccessful())
                return;
            var listReply = JSON.decode(responseList.getBody(), ZohoMessageListReply.class);
            if (listReply.data() == null || listReply.data().isEmpty())
                return;

            // Agrupar mensagens por folderId (A API do Zoho exige apagar por folder)
            Map<String, List<String>> messagesByFolder = listReply.data().stream()
                    .collect(Collectors.groupingBy(
                            ZohoMessageMetadata::folderId,
                            Collectors.mapping(ZohoMessageMetadata::messageId, Collectors.toList())));

            for (Map.Entry<String, List<String>> entry : messagesByFolder.entrySet()) {
                deleteMessagesInFolder(entry.getKey(), entry.getValue(), accessToken);
            }
        }
    }

    // --- Método Utilitário para a API de Apagar do Zoho ---

    private void deleteMessagesInFolder(String folderId, List<String> messageIds, OAuth2AccessToken token)
            throws Exception {
        String urlDelete = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/folders/" + folderId + "/messages";
        OAuthRequest requestDelete = new OAuthRequest(Verb.DELETE, urlDelete);
        requestDelete.addHeader("Content-Type", "application/json");

        // A API de DELETE do Zoho espera um array de IDs no body: ["id1", "id2"]
        requestDelete.setPayload(JSON.encode(messageIds));
        service.signRequest(token, requestDelete);

        try (Response responseDelete = service.execute(requestDelete)) {
            if (!responseDelete.isSuccessful()) {
                System.err.println("Erro ao apagar email no folder " + folderId + ": " + responseDelete.getBody());
            }
        }
    }
}
