package org.menski.zdb;

import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.deployment.PersistedWorkflow;
import io.zeebe.engine.state.instance.JobRecordValue;
import java.io.File;
import org.agrona.concurrent.UnsafeBuffer;

public class WorkflowDb {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println(
          "Usage: java -cp zeebe-db-reader-1.0-SNAPSHOT-jar-with-dependencies.jar org.menski.zdb.WorkflowDb PATH_TO_DB WORKFLOW_KEY");
      System.exit(1);
    }

    File dbPath = new File(args[0]);
    long workflowKey = Long.parseLong(args[1]);

    final Database database = new Database(dbPath);

    try {
      database.open();
      final byte[] bytes = database.get(ZbColumnFamilies.WORKFLOW_CACHE, workflowKey);
      if (bytes == null) {
        System.err.println(
            "Failed to find workflow with key "
                + workflowKey
                + " in database at path "
                + dbPath);
        System.exit(2);
      }
      final PersistedWorkflow persistedWorkflow = readPersistedWorkflow(bytes);
      System.out.println(persistedWorkflow);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read workflow", e);
    } finally {
      database.close();
    }
  }

  private static PersistedWorkflow readPersistedWorkflow(byte[] bytes) {
    final PersistedWorkflow persistedWorkflow = new PersistedWorkflow();
    persistedWorkflow.wrap(new UnsafeBuffer(bytes));
    return persistedWorkflow;
  }

}
