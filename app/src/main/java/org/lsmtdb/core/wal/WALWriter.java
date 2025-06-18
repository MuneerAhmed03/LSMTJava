package org.lsmtdb.core.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.util.concurrent.locks.ReentrantLock;

public class WALWriter implements Closeable {

  private final FileChannel channel;
  private final ReentrantLock lock = new ReentrantLock();
  private final int batchSize;
  private int currentBatchCount = 0;

  public WALWriter(Path walPath, int batchSize) throws IOException {
    Path parent = walPath.getParent();
    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent);
    }
    
    if (!Files.exists(walPath)) {
      Files.createFile(walPath);
    }
    
    this.channel =
        FileChannel.open(
            walPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    this.batchSize = batchSize;
  }

  public void append(WalEntry entry) throws IOException {
    lock.lock();
    try {
      int size = entry.serializedSize();
      ByteBuffer buffer = ByteBuffer.allocate(size);
      entry.serialize(buffer);
      buffer.flip();
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      currentBatchCount++;
      if (currentBatchCount >= batchSize) {
        flush();
      }
    } finally {
      lock.unlock();
    }
  }

  public void flush() throws IOException {
    channel.force(false);
    currentBatchCount = 0;
  }

  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      flush();
      channel.close();
    } finally {
      lock.unlock();
    }
  }

  public void clear() throws IOException {
    lock.lock();
    try {
      channel.truncate(0);
      currentBatchCount = 0;
    } finally {
      lock.unlock();
    }
  }
}
