package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.Arrays;

public class Q4_Job {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



//        tconst (string) - alphanumeric unique identifier of the title.
//        titleType (string) – the type/format of the title (e.g. movie, short,tvseries, tvepisode, video, etc).
//        primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release.
//        originalTitle (string) - original title, in the original language.
//        isAdult (boolean) - 0: non-adult title; 1: adult title.
//        startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year.
//        endYear (YYYY) – TV Series end year. for all other title types.
//        runtimeMinutes – primary runtime of the title, in minutes.
//        genres (string array) – includes up to three genres associated with the title.


        DataSource<Tuple9<String, String, String, String, String, String, String, String, String>> title_basics= env.readCsvFile("C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\title.basics.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class, String.class, String.class,  String.class);

        var title_basics_filtered = title_basics
                .filter(item -> !item.f5.equals("startYear")&&!item.f5.equals("\\N"))
                .map(item -> new Tuple9<String, String, String, String,String,Integer, String, String, String>(item.f0, item.f1, item.f2, item.f3, item.f4, Integer.parseInt(item.f5), item.f6, item.f7, item.f8)).returns(Types.TUPLE(Types.STRING, Types.STRING,Types.STRING,Types.STRING,Types.STRING, Types.INT, Types.STRING,Types.STRING, Types.STRING))
                .filter(item -> item.f5<1950);
//        tconst (string) - alphanumeric unique identifier of the title.
//        ordering (integer) – a number to uniquely identify rows for a given titleId.
//        nconst (string) - alphanumeric unique identifier of the name/person.
//        category (string) - the category of job that person was in.
//        job (string) - the specific job title if applicable, else.
//        characters (string) - the name of the character played if applicable, else.
        DataSource<Tuple3<String, String, String>> title_ratings = env.readCsvFile("C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\title.ratings.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class);

        var title_ratings_filtered = title_ratings
                .filter(item -> !item.f0.equals("tconst"))
                .map(item-> new Tuple3<String, Float, Integer>(item.f0,Float.parseFloat(item.f1),Integer.parseInt(item.f2))).returns(Types.TUPLE(Types.STRING, Types.FLOAT, Types.INT))
                .filter(item -> item.f1>8.5&&item.f2>10);

        var join = title_basics_filtered
                .join(title_ratings_filtered)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectFirst(2)
                .projectSecond(1,2)
                ;//.returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));



        // All the occurrences of actors from the 1960's
//        var name_basics_filtered =  name_basics
//                .filter(item -> !item.f2.equals("\\N") && !item.f3.equals("\\N"))
//                .filter(item -> item.f2.equals("1960"))
////                .filter(item -> item.f3.equals("2000"))
//                .map(item -> new Tuple6<String, String, Integer, Integer, String[], String>(item.f0, item.f1, Integer.parseInt(item.f2), Integer.parseInt(item.f3), item.f4.split(","), item.f5)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.OBJECT_ARRAY(Types.STRING), Types.STRING))
////                .filter(item -> item.f3<1990)
//                .filter(item -> Arrays.stream(item.f4).anyMatch(x -> x.equals("actor")));
//
//        var join = title_prinicipals
//                .join(name_basics_filtered)
//                .where(item -> item.f2)
//                .equalTo(item -> item.f0)
//                .projectSecond(1).projectFirst(5)
//                ;//.returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));
//
//        // Get all the roles from actors, as well as the number of roles played
//        var query = //join.map(item -> new Tuple2<String, String>(item.getField(0), item.getField(1))).returns(Types.TUPLE(Types.STRING, Types.STRING));
//        join.map(item -> new Tuple3<String, String, Integer>(item.getField(0), item.getField(1),1)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
//                .map(item -> new Tuple3<String, ArrayList<String>, Integer>(item.f0,new ArrayList<String>(Arrays.asList(item.f1)), item.f2)).returns(Types.TUPLE(Types.STRING, Types.LIST(Types.STRING), Types.INT))
//                .groupBy(0)
//                .reduce((item1, item2) ->{
//                    ArrayList<String> res = new ArrayList<String>();
//                    res.addAll(item1.f1);
//                    res.addAll(item2.f1);
//                    return new Tuple3<String, ArrayList<String>, Integer>(item1.f0,res,item1.f2+item2.f2);
//                });
//        var query = join.map(item -> new Tuple3<String, String[], Integer>(item.f0, itemf.f1, 1)).returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING),Types.INT));
//                .groupBy(0)
//                        (item1, item2) -> {


        // Get all sorted unoriginal transliterated greek titles, merged into one list by the amount of entries they have.
//        var query = title_akas
//                //Map to desired attributes
//                .map(item -> new Tuple5<String, String, String, String, String>(item.f1, item.f2, item.f3, item.f6, item.f7)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING))
//                //Filter null values
//                .filter(item -> !item.f1.equals("\\N") && !item.f2.equals("\\N"))
//                // Greek titles
//                .filter(item -> item.f2.equals("GR"))
//                //Unoriginal titles
//                .filter(item -> item.f4.equals("0"))
//                //Split properties
//                .map(item -> new Tuple5<String, String, String, String[], String>(item.f0, item.f1, item.f2, item.f3.split(" "), item.f4)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.OBJECT_ARRAY(Types.STRING), Types.STRING))
//                //Get transliterated
//                .filter(item -> Arrays.stream(item.f3).anyMatch(x -> x.equals("transliterated")))
//                //Size down to tuple2, adjust for merging titles
//                .map(item -> new Tuple2<Integer, ArrayList<String>>(Integer.parseInt(item.f0), new ArrayList<String>(Arrays.asList(item.f1)))).returns(Types.TUPLE(Types.INT, Types.LIST(Types.STRING)))
//                //Group by entry size
//                .groupBy(item -> item.f0)
//                //Reduce to one list of titles for each entry number
//                .reduce((item1, item2) -> {
//                    ArrayList<String> res = new ArrayList<String>();
//                    res.addAll(item1.f1);
//                    res.addAll(item2.f1);
//                    return new Tuple2<Integer,
//                            ArrayList<String>>(item1.f0, res);
//                }).returns(Types.TUPLE(Types.INT, Types.LIST(Types.STRING)))
//                //Sort
//                .sortPartition(0, Order.ASCENDING).setParallelism(1);
//
        var collected = join.collect();
        collected.forEach(System.out::println);
    }
}
