package org.menski.zdb;

import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.engine.state.instance.JobRecordValue;
import java.io.File;
import java.lang.reflect.Constructor;
import org.agrona.concurrent.UnsafeBuffer;

public class JobsDb {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println(
          "Usage: java -cp zeebe-db-reader-1.0-SNAPSHOT-jar-with-dependencies.jar org.menski.zdb.JobsDb PATH_TO_DB JOB_KEY");
      System.exit(1);
    }

    File dbPath = new File(args[0]);
    long jobKey = Long.parseLong(args[1]);

    final Database database = new Database(dbPath);

    try {
      database.open();
      final byte[] bytes = database.get(ZbColumnFamilies.JOBS, jobKey);
      if (bytes == null) {
        System.err.println(
            "Failed to find job with key "
                + jobKey
                + " in database at path "
                + dbPath);
        System.exit(2);
      }
      final JobRecordValue jobRecordValue = readJobRecordValue(bytes);
      System.out.println(jobRecordValue);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read job", e);
    } finally {
      database.close();
    }
  }

  private static JobRecordValue readJobRecordValue(byte[] bytes) {
    final JobRecordValue jobRecordValue = new JobRecordValue();
    jobRecordValue.wrap(new UnsafeBuffer(bytes));
    return jobRecordValue;
  }

}
