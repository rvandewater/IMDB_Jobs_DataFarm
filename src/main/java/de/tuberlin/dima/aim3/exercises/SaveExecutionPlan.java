package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Paths;

public class SaveExecutionPlan {
    String outputPath = "C:\\Users\\Robin\\Documents\\experimentdata\\IMDB_Jobs_Output";

    String dataPath = "";
    String execPlanOutPath = "";
    String execute="";
    String heap_size="";
    String execPlan;
    JobExecutionResult execResult;
    Long runTime;

    SaveExecutionPlan(){

    }
    public void GetExecutionPlan(ExecutionEnvironment env) throws Exception {
        this.execPlan = env.getExecutionPlan();
        this.execResult = env.execute();
        this.runTime = execResult.getNetRuntime();
        System.out.println("Runtime execution: "+execResult.getNetRuntime());
    }

    public void SaveExecutionPlan(String jobID, ExecutionEnvironment env) throws FileNotFoundException {
        var outFilePath = Paths.get(outputPath,jobID+".json");
        var pw = new PrintWriter(new File(outFilePath.toString()));
        var config = env.getConfiguration();
        var reachExecPlan = "{\n" +
                "\t\"netRunTime\": " + String.format("%s",runTime) + ",\n" +
                "\t\"executionPlan\": "+ String.format("%s",execPlan) + "\n" +"}";
//                "\t\"execPlanOutPath\": \"" + s"${params("execPlanOutPath")}" + "\",\n" +
//                "\t\"execute\": \"" + s"${params("execute")}" + "\",\n" +
//                "\t\"local\": \"" + s"${params("local")}" + "\",\n" +
//                "\t\"heap_size\": \"" + s"${params("heap_size")}" + "\",\n" +
//                "\t\"netRunTime\": " + s"${execTime.getOrElse(-1)},\n" +
//                "\t\"environmentConfig\": \"" + s"${env.getJavaEnv.getConfiguration.toString}" + "\",\n" +
//                "\t\"executionConfig\": \"" + s"${env.getConfig.toString}" + "\",\n" +
//                "\t\"executionPlan\": " + s"$execPlan\n" +
//                "}"
        pw.write(reachExecPlan);
        pw.close();

    }
}
