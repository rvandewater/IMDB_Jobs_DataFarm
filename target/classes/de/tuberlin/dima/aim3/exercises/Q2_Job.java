package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q2_Job {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple8<String, String, String, String, String, String, String, String>> title_akas = env.readCsvFile("C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\title.akas.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class, String.class, String.class);
        // Get all sorted unoriginal transliterated greek titles, merged into one list by the amount of entries they have.
        var query = title_akas
                //Map to desired attributes
                .map(item -> new Tuple5<String, String, String, String, String>(item.f1, item.f2, item.f3, item.f6, item.f7)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING))
                //Filter null values
                .filter(item -> !item.f1.equals("\\N") && !item.f2.equals("\\N"))
                // Greek titles
                .filter(item -> item.f2.equals("GR"))
                //Unoriginal titles
                .filter(item -> item.f4.equals("0"))
                //Split properties
                .map(item -> new Tuple5<String, String, String, String[], String>(item.f0, item.f1, item.f2, item.f3.split(" "), item.f4)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.OBJECT_ARRAY(Types.STRING), Types.STRING))
                //Get transliterated
                .filter(item -> Arrays.stream(item.f3).anyMatch(x -> x.equals("transliterated")))
                //Size down to tuple2, adjust for merging titles
                .map(item -> new Tuple2<Integer, ArrayList<String>>(Integer.parseInt(item.f0), new ArrayList<String>(Arrays.asList(item.f1)))).returns(Types.TUPLE(Types.INT, Types.LIST(Types.STRING)))
                //Group by entry size
                .groupBy(item -> item.f0)
                //Reduce to one list of titles for each entry number
                .reduce((item1, item2) -> {
                    ArrayList<String> res = new ArrayList<String>();
                    res.addAll(item1.f1);
                    res.addAll(item2.f1);
                    return new Tuple2<Integer,
                            ArrayList<String>>(item1.f0, res);
                }).returns(Types.TUPLE(Types.INT, Types.LIST(Types.STRING)))
                //Sort
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

        var collected = query.collect();
        collected.forEach(System.out::println);
    }
}
