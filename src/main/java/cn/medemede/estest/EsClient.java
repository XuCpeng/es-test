package cn.medemede.estest;

import lombok.Setter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;


/**
 * @author xcp
 */
@Setter
public class EsClient {

    private static final Logger LOG = LogManager.getLogger();

    /**
     * Elasticsearch 的HOST，需要与生成 ca.crt 时一致
     */
    private static String elasticHost = "es01";

    private static String caCrtPath = "/bundle/ca/ca.crt";

    private static String elasticPassword = "Wdwculv5A2bIli3TyOb3";

    private static String elasticUsername = "elastic";

    private static volatile RestHighLevelClient restHighLevelClient = null;

    public static RestHighLevelClient getClient() throws CertificateException, URISyntaxException, KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {
        if (restHighLevelClient == null) {
            synchronized (EsClient.class) {
                if (restHighLevelClient == null) {
                    //设置https证书
                    Path caCertificatePath = Paths.get(EsClient.class.getResource(caCrtPath).toURI());
                    CertificateFactory factory = CertificateFactory.getInstance("X.509");
                    Certificate trustedCa;
                    try (InputStream is = Files.newInputStream(caCertificatePath)) {
                        trustedCa = factory.generateCertificate(is);
                    }
                    KeyStore trustStore = KeyStore.getInstance("pkcs12");
                    trustStore.load(null, null);
                    trustStore.setCertificateEntry("ca", trustedCa);
                    SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
                    final SSLContext sslContext = sslContextBuilder.build();

                    // 设置用户名密码
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(
                            AuthScope.ANY,
                            new UsernamePasswordCredentials(elasticUsername, elasticPassword));

                    // 组合 builder
                    RestClientBuilder builder = RestClient.builder(new HttpHost(elasticHost, 9200, "https"));
                    builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setSSLContext(sslContext).setDefaultCredentialsProvider(credentialsProvider));

                    restHighLevelClient = new RestHighLevelClient(builder);

                    LOG.info("RestHighLevelClient build successful!");
                }
            }
        }

        return restHighLevelClient;
    }


    private static void close() {
        if (restHighLevelClient == null) {
            return;
        }
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
