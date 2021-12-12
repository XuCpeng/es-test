package cn.medemede.estest;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * @author Admin03
 */

public class IndexDao {

    private RestHighLevelClient restHighLevelClient = null;

    public IndexDao() {
        try {
            restHighLevelClient = EsClient.getClient();
        } catch (CertificateException | URISyntaxException | KeyStoreException | NoSuchAlgorithmException | IOException | KeyManagementException e) {
            e.printStackTrace();
        }
    }

    public boolean createIndex(String index) {
        try {
            AcknowledgedResponse response = restHighLevelClient.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean deleteIndex(String index) {
        try {
            AcknowledgedResponse response = restHighLevelClient.indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public GetIndexResponse getIndex(String index) {
        try {
            return restHighLevelClient.indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
