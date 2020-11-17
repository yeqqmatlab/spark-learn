package ml;

import bean.AssociationMethod;
import bean.MethodJson;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * yqq 2020/11/17
 * 关联考点处理
 */
public class MethodJsonSpark {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("FPGrowthMethods")
                .master("local[2]")
                .getOrCreate();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "zsy");
        connectionProperties.put("password", "lc12345");

        String url = "jdbc:mysql://192.168.1.210:3307/data_mining?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&rewriteBatchedStatements=true";

        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> associationMethodDF = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "association_method")
                .option("user", "zsy")
                .option("password", "lc12345")
                .load();

        associationMethodDF.createOrReplaceTempView("method");

        Dataset<Row> methodListDF = spark.sql("select cast(method_id as int),collect_list(association_id) from method group by method_id");

        HashMap<Integer, List<Integer>> mapKeyList = new HashMap<>();
        List<Row> collect = methodListDF.toJavaRDD().collect();
        for (Row row : collect) {
            mapKeyList.put(row.getInt(0),row.getList(1));
        }



        List<Row> rows = associationMethodDF.collectAsList();

        List<AssociationMethod> list = rows.stream().map(r -> {
            int associationId = r.getInt(0);
            int methodId = Integer.parseInt(r.getString(1));
            int associationNum = r.getInt(2);
            String methodName = r.getString(3);
            AssociationMethod vo = new AssociationMethod();
            vo.setAssociationId(associationId);
            vo.setMethodId(methodId);
            vo.setAssociationNum(associationNum);
            vo.setMethodName(methodName);
            if (methodId == 827) {
                System.out.println(vo);
            }
            return vo;
        }).collect(Collectors.toList());

        List<Row> listRow = new ArrayList<>();
        HashMap<Integer, String> methodMapKV = new HashMap<>();
        mapKeyList.forEach((methodId,associationIdList) -> {
            List<MethodJson> methodJsonArrayList = new ArrayList<>();
            for (Integer associationId : associationIdList) {

                for (AssociationMethod associationMethod : list) {

                    int methodId1 = associationMethod.getMethodId();
                    int associationId1 = associationMethod.getAssociationId();

                    if(associationId.equals(associationId1) && !methodId.equals(methodId1)){
                        MethodJson methodJson = new MethodJson();
                        methodJson.setMethodId(methodId1);
                        methodJson.setAssociationNum(associationMethod.getAssociationNum());
                        methodJsonArrayList.add(methodJson);
                    }

                }

            }
            methodMapKV.put(methodId, JSONArray.toJSONString(methodJsonArrayList));
            /*System.out.println(methodId);
            System.out.println(JSONArray.toJSONString(methodJsonArrayList));*/
            Row row = RowFactory.create(methodId, JSONArray.toJSONString(methodJsonArrayList));
            listRow.add(row);
        });


        List<StructField> schemaList = Lists.newArrayList();
        schemaList.add(DataTypes.createStructField("method_id", DataTypes.IntegerType, true));
        schemaList.add(DataTypes.createStructField("association_method", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(schemaList);
        Dataset<Row> associationDF = spark.createDataFrame(listRow, schema);

        associationDF.write()
                .mode(SaveMode.Overwrite)
                .option("truncate","true")
                .jdbc(url, "association_method_json", connectionProperties);

        spark.stop();
    }
}
