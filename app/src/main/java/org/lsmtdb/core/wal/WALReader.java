package org.lsmtdb.core.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.List;

public class WALReader implements Closeable {

  private final FileChannel channel;
  private final ReentrantLock lock = new ReentrantLock();
  private final long size;

  public WALReader(Path walPath) throws IOException {
    this.channel = FileChannel.open(walPath);
    this.size = channel.size();
  }

  public List<WalEntry> recover() throws IOException {
    lock.lock();
    try {
      List<WalEntry> entries = new ArrayList<>();
      channel.position(0);

      if (this.size == 0) {
        return entries;
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) this.size);
      channel.read(buffer);
      buffer.flip();
      while (buffer.hasRemaining()) {
        try {
          WalEntry entry = WalEntry.deserialize(buffer);
          entries.add(entry);
        } catch (BufferUnderflowException e) {
          break;
        }
      }
      return entries;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
