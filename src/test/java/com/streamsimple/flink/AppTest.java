package com.streamsimple.flink;

import com.streamsimple.javautils.testutils.DirTestWatcher;
import com.streamsimple.javautils.testutils.ResourceUtils;
import java.nio.file.Paths;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Rule;
import org.junit.Test;

public class AppTest
{
  @Rule
  public final DirTestWatcher dirTestWatcher = new DirTestWatcher.Builder()
      .setDeleteAtEnd(false)
      .build();

  @Test
  public void simpleTest() throws Exception
  {
    final String filePath = ResourceUtils.getResourceAsFile(Paths.get("test.txt")).getAbsolutePath();

    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);
    DataSet<String> data = env.readTextFile(filePath);

    data
        .filter(new FilterFunction<String>() {
          public boolean filter(String value) {
            return value.startsWith("a");
          }
        })
        .writeAsText(dirTestWatcher.getDir().getAbsolutePath() + "/testout.txt");

    JobExecutionResult res = env.execute();
  }
}
