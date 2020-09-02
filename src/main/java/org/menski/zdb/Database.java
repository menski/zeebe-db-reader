package org.menski.zdb;

import io.netty.buffer.ByteBufUtil;
import io.zeebe.engine.state.ZbColumnFamilies;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Database {

  private final String dbPath;
  private RocksDB rocksDB;
  private final Map<ZbColumnFamilies, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
  private final ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);

  public Database(File dbPath) {
    this.dbPath = dbPath.getAbsolutePath();
  }

  public void open() {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = getColumnFamilyDescriptors();
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try {
      rocksDB = RocksDB.openReadOnly(dbPath, columnFamilyDescriptors, columnFamilyHandles);
      createColumnFamilyHandlesMap(columnFamilyHandles);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to open rocksdb", e);
    }
  }

  public void close() {
    columnFamilyHandleMap.values().forEach(ColumnFamilyHandle::close);
    columnFamilyHandleMap.clear();
    if (rocksDB != null) {
      rocksDB.close();
      rocksDB = null;
    }
  }

  private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
    final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
        .setCompactionPriority(CompactionPriority.OldestSmallestSeqFirst);

    return Arrays.stream(ZbColumnFamilies.values())
        .map(v -> v.name().toLowerCase().getBytes())
        .map(name -> new ColumnFamilyDescriptor(name, columnFamilyOptions))
        .collect(Collectors.toList());
  }

  public byte[] get(ZbColumnFamilies columnFamily, long key) {
    final ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(columnFamily);
    if (columnFamilyHandle != null) {
      try {
        keyBuffer.putLong(key);
        return rocksDB.get(columnFamilyHandle, keyBuffer.array());
      } catch (RocksDBException e) {
        throw new RuntimeException("Failed to read key " + key + " from column family " + columnFamily.name(), e);
      }
    }
    else {
      throw new RuntimeException("Unknown column family name " + columnFamily.name());
    }
  }


  private void createColumnFamilyHandlesMap(final List<ColumnFamilyHandle> columnFamilyHandles) {
    final ZbColumnFamilies[] values = ZbColumnFamilies.values();
    for (int i = 0; i < values.length; i++) {
      columnFamilyHandleMap.put(values[i], columnFamilyHandles.get(i));
    }
  }
}
