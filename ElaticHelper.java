package cn.thinkjoy.etl.elastic;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaochanghong on 15/8/28.
 */
public class ElaticHelper implements Serializable {
    private Logger logger = LoggerFactory.getLogger(ElaticHelper.class);

    private static TransportClient client = null;


    public ElaticHelper(ElasticConf elasticConf) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticConf.getClusterName()).build();
        client = new TransportClient(settings);

        if (elasticConf.getIpAndPorts() == null || elasticConf.getIpAndPorts().size() == 0) {
            throw new Exception("elastic connection address is empty,please check.");
        }
        for (String ipAndPort : elasticConf.getIpAndPorts()) {
            String ip = ipAndPort.split(":")[0];
            int port = Integer.parseInt(ipAndPort.split(":")[1]);
            client.addTransportAddress(new InetSocketTransportAddress(ip, port));
        }

        logger.info("elastic helper conntect to es cluster,connected nodes:" + client.connectedNodes());
    }


    /**
     * 添加一个文档，文档id自动生成
     *
     * @param indexName 索引名称
     * @param type      类型
     * @param json      文档json字符串
     * @return
     * @throws Exception
     */
    public IndexResponse addDocument(String indexName, String type, String json) throws Exception {
        return addDocument(indexName, type, null, json);

    }


    /**
     * 添加一个文档，指定文档id
     *
     * @param indexName 索引名称
     * @param type      类型
     * @param id        文档那个id
     * @param json      文档json串
     * @return
     * @throws Exception
     */
    public IndexResponse addDocument(String indexName, String type, String id, String json) throws Exception {

        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, type);
        if (!this.isEmpty(id)) {
            indexRequestBuilder.setId(id);
        }
        indexRequestBuilder.setCreate(true);
        indexRequestBuilder.setSource(json);
        logger.info("add a document,json:" + indexRequestBuilder.toString());
        IndexResponse response = indexRequestBuilder.execute().actionGet();
        return response;

    }

    /**
     * 添加或替换一个文档，指定文档id，如果添加的文档已经存在则替换
     *
     * @param indexName 索引名称
     * @param type      类型
     * @param id        文档那个id
     * @param json      文档json串
     * @return
     * @throws Exception
     */
    public IndexResponse addOrReplaceDocument(String indexName, String type, String id, String json) throws Exception {

        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, type);
        if (!this.isEmpty(id)) {
            indexRequestBuilder.setId(id);
        }
        indexRequestBuilder.setSource(json);
        indexRequestBuilder.setCreate(false);
        logger.info("add a document,json:" + indexRequestBuilder.toString());
        IndexResponse response = indexRequestBuilder.execute().actionGet();
        return response;

    }

    /**
     * 删除一个文档
     *
     * @param indexName 索引名称
     * @param type      类型
     * @param _id       文档id
     * @return
     * @throws Exception
     */
    public DeleteResponse deleteDocument(String indexName, String type, String _id) throws Exception {

        DeleteResponse response = client.prepareDelete(indexName, type, _id)
                .execute()
                .actionGet();
        return response;
    }


    /**
     * 更新文档，仅限于文档顶级的属性
     *
     * @param indexName 索引名称
     * @param type      类型
     * @param _id       待更新的文档id
     * @param source    存放顶级属性和值，例如Map("student_id","11111111")
     * @return
     * @throws Exception
     */
    public UpdateResponse updateDocument(String indexName, String type, String _id, Map<String, String> source) throws Exception {

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
        for (Map.Entry<String, String> me : source.entrySet()) {
            xContentBuilder.field(me.getKey(), me.getValue());
        }
        xContentBuilder.endObject();
        UpdateResponse response = client.prepareUpdate(indexName, type, _id).setDoc().setDoc(xContentBuilder).get();
        return response;
    }

    public UpdateResponse updateDocument(String indexName, String type, String _id, String updatedJson) throws Exception {

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(indexName, type, _id);
        UpdateResponse response = updateRequestBuilder.setDoc().setDoc(updatedJson).get();
        return response;
    }

    /**
     * 基于and条件的查询
     *
     * @param indexName 索引名称
     * @param cond      查询条件，key存放属性名称，value为属性值
     * @return
     * @throws Exception
     */
    public List<String> andTermSearch(String indexName, Map<String, String> cond) throws Exception {
        return this.andOrTermSearch(indexName, null, cond, true);
    }


    /**
     * 基于or条件的查询
     *
     * @param indexName 索引名称
     * @param cond      查询条件，key存放属性名称，value为属性值
     * @return
     * @throws Exception
     */
    public List<String> orTermSearch(String indexName, Map<String, String> cond) throws Exception {
        return this.andOrTermSearch(indexName, null, cond, true);
    }


    public List<ESResponse> andTermSearch(String indexName, String type, Map<String, String> cond,boolean containSource) throws Exception {
        return this.andOrTermSearch(indexName, type, cond, true, containSource);
    }


    public List<String> andTermSearch(String indexName, String type, Map<String, String> cond) throws Exception {
        return this.andOrTermSearch(indexName, type, cond, true);
    }

    public List<String> orTermSearch(String indexName, String type, Map<String, String> cond) throws Exception {
        return this.andOrTermSearch(indexName, type, cond, true);
    }

    private List<String> andOrTermSearch(String indexName, String type, Map<String, String> cond, boolean must) throws Exception {

        List<String> searchResults = new ArrayList<String>();
        List<ESResponse> responses = this.andOrTermSearch(indexName, type, cond, must, true);
        for (ESResponse esResponse : responses) {
            searchResults.add(esResponse.getSource());
        }
        return searchResults;
    }


    private List<ESResponse> andOrTermSearch(String indexName, String type, Map<String, String> cond, boolean must, boolean containSource) throws Exception {

        List<ESResponse> searchResults = new ArrayList<ESResponse>();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName);
        if (!this.isEmpty(type)) {
            searchRequestBuilder.setTypes(type);
        }

        List<TermsFilterBuilder> termsFilterBuilders = new ArrayList<TermsFilterBuilder>();
        for (Map.Entry<String, String> me : cond.entrySet()) {
            termsFilterBuilders.add(new TermsFilterBuilder(me.getKey(), me.getValue()));
        }

        TermsFilterBuilder[] termsFilterBuildersArray = new TermsFilterBuilder[]{};

        BoolFilterBuilder builter = null;
        if (must) {
            builter = FilterBuilders.boolFilter().must(termsFilterBuilders.toArray(termsFilterBuildersArray));

        } else {
            builter = FilterBuilders.boolFilter().should(termsFilterBuilders.toArray(termsFilterBuildersArray));
        }

        searchRequestBuilder.setPostFilter(builter);

        try {
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            SearchHits hits = response.getHits();

            for (SearchHit hit : hits.getHits()) {
                ESResponse esResponse = new ESResponse();
                esResponse.setIndexName(hit.getIndex());
                esResponse.setType(hit.getType());
                esResponse.setId(hit.getId());
                if (containSource) {
                    esResponse.setSource(hit.getSourceAsString());
                }
                searchResults.add(esResponse);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return searchResults;
    }

    public void close() throws Exception {
        client.close();
    }

    private boolean isEmpty(String str) {
        if (str != null && !"".endsWith(str)) {
            return false;
        }
        return true;
    }
}
