package cn.thinkjoy.etl.elastic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaochanghong on 15/8/28.
 */
public class ESDemo {
    public static void main(String[] args) throws Exception {
        ElasticConf elasticConf = new ElasticConf("elasticsearch", "10.254.212.167:9300");
        ElaticHelper helper = new ElaticHelper(elasticConf);

        String document = "{" +
                "               \"rank\": \"2\"," +
                "               \"student_id\": \"6455\"," +
                "               \"class_grade\": \"4\"," +
                "               \"history_average_score\": 77.3," +
                "               \"committed_sentence_count\": 5" +
                "            }";

        String document1 = "{" +
                "  \"bizData\": {" +
                "    \"total\": 2," +
                "    \"result\": [" +
                "      {" +
                "        \"student_id\": 23234," +
                "        \"textbook_unit\": \"1Hereyouare.\"," +
                "        \"finished_date\": \"2015-08-2214:33:45\"," +
                "        \"subject_type\": 1," +
                "        \"sentence\": \"Otherstudentswaragainsttheirplan.\"," +
                "        \"score\": 89.3" +
                "      }"+
                "    ]" +
                "  }," +
                "  \"rtnCode\": \"0000000\"," +
                "  \"ts\": 1440562252868" +
                "}";

        //插入
//        helper.addDocument("idx1", "type6", "7", document);

        //修改
        Map<String,String> updateMap = new HashMap<String, String>();
//        updateMap.put("class_grade","6");
        updateMap.put("total","9000");
//        helper.updateDocument("idx1","type6","7",updateMap);

        //查询
        Map<String,String> paramMap = new HashMap<String, String>();
        paramMap.put("_id","id1");
        //searchAndPrint(helper,paramMap);

        //模糊查询
        Map<String,String> paramMap1 = new HashMap<String, String>();
        paramMap1.put("rank","1");
        fuzzyearchAndPrint(helper,paramMap1);

        //删除
        //helper.deleteDocument("idx1","type1","id1");

        helper.close();
    }

    public static void searchAndPrint(ElaticHelper helper,Map<String, String> param) {
        try {
            List<String> result = helper.andTermSearch("idx1","type1",param);
            System.out.println(result.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void fuzzyearchAndPrint(ElaticHelper helper,Map<String, String> param) {
        try {
            List<ESResponse> result = helper.andTermSearch("yzt_errornotes_*","errornotes",param,false);
            for(ESResponse response : result){
                System.out.println("indexName:"+response.getIndexName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
