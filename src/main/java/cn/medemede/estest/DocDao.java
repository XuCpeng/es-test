package cn.medemede.estest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * @author Admin03
 */
public class DocDao {
    private final ObjectMapper mapper;

    private RestHighLevelClient restHighLevelClient = null;

    public DocDao() {
        mapper = new ObjectMapper();
        try {
            restHighLevelClient = EsClient.getClient();
        } catch (CertificateException | URISyntaxException | KeyStoreException | NoSuchAlgorithmException | IOException | KeyManagementException e) {
            e.printStackTrace();
        }
    }

    public IndexResponse insertDoc(String index, Object val, String id) {
        try {
            IndexRequest request = new IndexRequest();
            request.index(index);
            if (id != null) {
                request.id(id);
            }
            request.source(mapper.writeValueAsBytes(val), XContentType.JSON);
            return restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public UpdateResponse updateDoc(String index, String id, String item, String newVal) {
        try {
            UpdateRequest request = new UpdateRequest();
            request.index(index).id(id).doc(XContentType.JSON, item, newVal);
            return restHighLevelClient.update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public GetResponse getDoc(String index, String id) {
        try {
            GetRequest request = new GetRequest();
            request.index(index).id(id);
            return restHighLevelClient.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public DeleteResponse deleteDoc(String index, String id) {
        try {
            DeleteRequest request = new DeleteRequest();
            request.index(index).id(id);
            return restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
