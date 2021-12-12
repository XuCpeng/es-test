package cn.medemede.estesttest;

import cn.medemede.estest.DocDao;
import cn.medemede.estest.IndexDao;
import cn.medemede.estest.User;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.jupiter.api.*;

import java.util.Arrays;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EsClientTest {

    private static final String TEST_INDEX = "user";
    private final IndexDao indexDao = new IndexDao();
    private final DocDao docDao = new DocDao();

    @Test
    @Order(1)
    void createIndexTest() {
        IndexDao indexDao = new IndexDao();
        boolean res = indexDao.createIndex(TEST_INDEX);
        Assertions.assertTrue(res);
    }


    @Test
    @Order(2)
    void getIndexTest() {
        GetIndexResponse response = indexDao.getIndex(TEST_INDEX);
        Assertions.assertNotNull(response);
        System.out.println(response.getAliases());
        System.out.println(Arrays.toString(response.getIndices()));
        System.out.println(response.getMappings());
        System.out.println(response.getSettings());
    }

    @Test
    @Order(3)
    void insertDocTest() {
        IndexResponse response = docDao.insertDoc(TEST_INDEX, new User("散华礼弥", "男", 24), "1001");
        Assertions.assertNotNull(response);
        System.out.println(response);
    }

    @Test
    @Order(4)
    void updateDocTest() {
        UpdateResponse response = docDao.updateDoc(TEST_INDEX, "1001", "sex", "还是男");
        Assertions.assertNotNull(response);
        System.out.println(response);
    }

    @Test
    @Order(4)
    void getDocTest() {
        GetResponse response = docDao.getDoc(TEST_INDEX, "1001");
        Assertions.assertNotNull(response);
        System.out.println(response.getSourceAsString());
    }

    @Test
    @Order(5)
    void deleteDocTest() {
        DeleteResponse response = docDao.deleteDoc(TEST_INDEX, "1001");
        Assertions.assertNotNull(response);
        System.out.println(response);
    }

    @Test
    @Order(6)
    void deleteIndexTest() {
        boolean res = indexDao.deleteIndex(TEST_INDEX);
        Assertions.assertTrue(res);
    }
}
