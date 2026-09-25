package io.github.alikelleci.eventify.core.kafka;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.Map;

public class RocksDBConfig implements RocksDBConfigSetter {

  @Override
  public void setConfig(String s, Options options, Map<String, Object> map) {
    options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
  }

  @Override
  public void close(String storeName, Options options) {
    options.close();
  }
}
