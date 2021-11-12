package cn.medemede.estest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ESClient {
    private static final Logger LOG = LoggerFactory.getLogger(ESClient.class);

    public static void main(String[] args) throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {

        String pathStr = "/bundle/ca/ca.crt";

        //设置https证书
        Path caCertificatePath = Paths.get(ESClient.class.getResource(pathStr).toURI());
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

        //设置用户名密码
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "fhFE2NbkRvgf56q6fgw6"));

        // 组合 builder
        RestClientBuilder builder = RestClient.builder(new HttpHost("127.0.0.1", 9200, "https"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setSSLContext(sslContext).setDefaultCredentialsProvider(credentialsProvider));


        // 生成 Client
        boolean acknowledged;
        try (RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder)) {
            String index = "user2";
            AcknowledgedResponse response1 = restHighLevelClient
                    .indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
            AcknowledgedResponse response2 = restHighLevelClient
                    .indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
            acknowledged = response1.isAcknowledged() && response2.isAcknowledged();
        }

        if (acknowledged) {
            LOG.info("response success!");
        } else {
            LOG.debug("response failed!");
        }
    }
}
