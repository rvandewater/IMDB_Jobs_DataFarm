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

        //        tconst (string) - alphanumeric unique identifier of the title.
//        ordering (integer) – a number to uniquely identify rows for a given titleId.
//        nconst (string) - alphanumeric unique identifier of the name/person.
//        category (string) - the category of job that person was in.
//        job (string) - the specific job title if applicable, else.
//        characters (string) - the name of the character played if applicable, else.

        DataSource<Tuple3<String, String, String>> title_ratings = env.readCsvFile("C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\title.ratings.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class);

        // Parse and filter
        var title_basics_filtered = title_basics
                // Filter to remove unparsable lines
                .filter(item -> !item.f5.equals("startYear")&&!item.f5.equals("\\N"))
                // Parse to int
                .map(item -> new Tuple9<String, String, String, String,String,Integer, String, String, String>(item.f0, item.f1, item.f2, item.f3, item.f4, Integer.parseInt(item.f5), item.f6, item.f7, item.f8)).returns(Types.TUPLE(Types.STRING, Types.STRING,Types.STRING,Types.STRING,Types.STRING, Types.INT, Types.STRING,Types.STRING, Types.STRING))
                // Filter to startyear less than 1950
                .filter(item -> item.f5<1950);


        //Parse and Filter
        var title_ratings_filtered = title_ratings
                //Filter out unparsable
                .filter(item -> !item.f0.equals("tconst"))
                // Parse to float and int
                .map(item-> new Tuple3<String, Float, Integer>(item.f0,Float.parseFloat(item.f1),Integer.parseInt(item.f2))).returns(Types.TUPLE(Types.STRING, Types.FLOAT, Types.INT))
                // Rating at least 8.5, more than 10 reviews
                .filter(item -> item.f1>8.5&&item.f2>10);

        //Join tables
        var join = title_basics_filtered
                .join(title_ratings_filtered)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                // Get Movie/Show title
                .projectFirst(2)
                // Get rating and number of ratings
                .projectSecond(1,2)
                ;//.returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));

        var collected = join.collect();
        collected.forEach(System.out::println);
    }
}
