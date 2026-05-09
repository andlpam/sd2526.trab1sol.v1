package sd2526.trab.impl.db;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.impl.db.zoho.ZohoServiceFactory;
import sd2526.trab.impl.db.zoho.ZohoTokenManager;
import sd2526.trab.impl.db.zoho.msgs.ZohoAccount;
import sd2526.trab.impl.db.zoho.msgs.ZohoAccountReply;
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

    private Zoho() {
        service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    synchronized public static Zoho getInstance() {
        if (instance == null)
            instance = new Zoho();
        return instance;
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

}