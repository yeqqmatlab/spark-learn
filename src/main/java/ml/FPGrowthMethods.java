package ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import java.util.*;
import java.util.stream.Collectors;


public class FPGrowthMethods {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("FPGrowthMethods")
                .master("local[2]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> topicMethodsDF = spark.read().parquet("hdfs://ip243:8020/zsy/warehouse/ods/tiku/topic_methods");

        JavaRDD<Row> methodIdsRDD = topicMethodsDF
                .select("method_ids")
                .toJavaRDD()
                //.filter(row -> row.getString(0).contains(","))
                .filter(row ->{
                    Set<Integer> set = new HashSet<>();
                    String[] split = row.getString(0).split("[,]");
                    for (String s : split) {
                        set.add(Integer.parseInt(s));
                    }
                    return set.size() == split.length;
                });


        final List<Row> dataMethods =  methodIdsRDD.collect().stream().map(row -> RowFactory.create(Arrays.asList(row.getString(0).split("[,]")))).collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{ new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });

        Dataset<Row> itemsDF  = spark.createDataFrame(dataMethods, schema);

        final FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(0.0001)
                .setMinConfidence(0.7)
                .fit(itemsDF);

        // Display frequent itemsets.
//        model.freqItemsets().printSchema();
//        System.out.println("freqItemsets--->");
        model.freqItemsets().show();
        for (Row row : model.freqItemsets().toJavaRDD().collect()) {
            List<String> list = row.getList(0);
            long count = row.getLong(1);
            System.out.println(list.toString()+"--->"+count);
        }

        // Display generated association rules.
        model.associationRules().printSchema();
        System.out.println("associationRules--->");
        model.associationRules().show();

        for (Row row : model.associationRules().toJavaRDD().collect()) {
            List<String> antecedentList = row.getList(0);
            List<String> consequentList = row.getList(1);
            double confidence = row.getDouble(2);
            System.out.println(antecedentList.toString()+"===>"+consequentList.toString()+","+confidence);
        }



        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        //model.transform(itemsDF).show();

        spark.stop();
    }
}
