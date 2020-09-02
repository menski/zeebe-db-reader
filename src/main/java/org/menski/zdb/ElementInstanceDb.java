package org.menski.zdb;

import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.instance.ElementInstance;
import java.io.File;
import java.lang.reflect.Constructor;
import org.agrona.concurrent.UnsafeBuffer;

public class ElementInstanceDb {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println(
          "Usage: java -cp zeebe-db-reader-1.0-SNAPSHOT-jar-with-dependencies.jar org.menski.zdb.ElementInstanceDb PATH_TO_DB ELEMENT_INSTANCE_KEY");
      System.exit(1);
    }

    File dbPath = new File(args[0]);
    long elementInstanceKey = Long.parseLong(args[1]);

    final Database database = new Database(dbPath);

    try {
      database.open();
      final byte[] bytes = database.get(ZbColumnFamilies.ELEMENT_INSTANCE_KEY, elementInstanceKey);
      if (bytes == null) {
        System.err.println(
            "Failed to find element instances with key "
                + elementInstanceKey
                + " in database at path "
                + dbPath);
        System.exit(2);
      }
      final ElementInstance elementInstance = readElementInstance(bytes);
      System.out.println(elementInstance);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read element instance", e);
    } finally {
      database.close();
    }
  }

  private static ElementInstance readElementInstance(byte[] bytes) throws Exception {
    Constructor<ElementInstance> constructor = ElementInstance.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    final ElementInstance elementInstance = constructor.newInstance();
    elementInstance.wrap(new UnsafeBuffer(bytes));
    return elementInstance;
  }
}
