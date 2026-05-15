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
    static final String REFRESH_TOKEN = "1000.46a534cbabb2ee8c89fafce0fedfff38.ac38eacf791f811e57936ed924326268";

    private static final String ACCOUNTS = "/accounts";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;

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

    public void sendEmail(String subject, String body) throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        String url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages";
        OAuthRequest request = new OAuthRequest(Verb.POST, url);
        request.addHeader("Content-Type", "application/json");

        var emailPayload = new ZohoEmailRequest(primaryEmail, primaryEmail, subject, body, "plaintext");
        request.setPayload(JSON.encode(emailPayload));

        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("Erro a enviar email: " + response.getBody());
            }
        }
    }

    public List<String> getAllEmails() throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        List<String> emailBodies = new ArrayList<>();

        String urlList = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages/view";
        OAuthRequest requestList = new OAuthRequest(Verb.GET, urlList);
        service.signRequest(accessToken, requestList);

        try (Response responseList = service.execute(requestList)) {
            if (responseList.isSuccessful()) {
                var listReply = JSON.decode(responseList.getBody(), ZohoMessageListReply.class);
                if (listReply.data() == null)
                    return emailBodies;

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

    public void deleteEmailBySystemMid(String targetMid) throws Exception {
        ensureAccountDetails();
        var accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        String urlList = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages/view";
        OAuthRequest requestList = new OAuthRequest(Verb.GET, urlList);
        service.signRequest(accessToken, requestList);

        try (Response responseList = service.execute(requestList)) {
            if (!responseList.isSuccessful())
                return;
            var listReply = JSON.decode(responseList.getBody(), ZohoMessageListReply.class);
            if (listReply.data() == null)
                return;

            for (ZohoMessageMetadata meta : listReply.data()) {
                String urlContent = MAIL_API_BASE + ACCOUNTS + "/" + accountId +
                        "/folders/" + meta.folderId() + "/messages/" + meta.messageId() + "/content";

                OAuthRequest requestContent = new OAuthRequest(Verb.GET, urlContent);
                service.signRequest(accessToken, requestContent);

                try (Response responseContent = service.execute(requestContent)) {
                    if (responseContent.isSuccessful()) {
                        var contentReply = JSON.decode(responseContent.getBody(), ZohoMessageContentReply.class);
                        if (contentReply.data() != null && contentReply.data().content().contains(targetMid)) {

                            deleteMessagesInFolder(meta.folderId(), List.of(meta.messageId()), accessToken);
                            return;
                        }
                    }
                }
            }
        }
    }

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

            Map<String, List<String>> messagesByFolder = listReply.data().stream()
                    .collect(Collectors.groupingBy(
                            ZohoMessageMetadata::folderId,
                            Collectors.mapping(ZohoMessageMetadata::messageId, Collectors.toList())));

            for (Map.Entry<String, List<String>> entry : messagesByFolder.entrySet()) {
                deleteMessagesInFolder(entry.getKey(), entry.getValue(), accessToken);
            }
        }
    }

    private void deleteMessagesInFolder(String folderId, List<String> messageIds, OAuth2AccessToken token)
            throws Exception {
        for (String mid : messageIds) {
            // O ID da mensagem vai diretamente no URL
            String urlDelete = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/folders/" + folderId + "/messages/" + mid;
            OAuthRequest requestDelete = new OAuthRequest(Verb.DELETE, urlDelete);

            // Não é preciso .setPayload()!
            service.signRequest(token, requestDelete);

            try (Response responseDelete = service.execute(requestDelete)) {
                if (!responseDelete.isSuccessful()) {
                    System.err.println("Erro ao apagar email no folder " + folderId + ": " + responseDelete.getBody());
                }
            }
        }
    }
}
