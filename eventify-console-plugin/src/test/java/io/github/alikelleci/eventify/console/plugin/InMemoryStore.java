package io.github.alikelleci.eventify.console.plugin;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/** A key-value store in memory, sorted by key like the state stores, with the same inclusive ranges. For tests. */
class InMemoryStore<V> implements ReadOnlyKeyValueStore<String, V> {

  private final NavigableMap<String, V> entries = new TreeMap<>();

  void put(String key, V value) {
    entries.put(key, value);
  }

  void delete(String key) {
    entries.remove(key);
  }

  @Override
  public V get(String key) {
    return entries.get(key);
  }

  @Override
  public KeyValueIterator<String, V> range(String from, String to) {
    if (from.compareTo(to) > 0) {
      throw new IllegalArgumentException("Range from " + from + " is after its end " + to);
    }
    return iterator(entries.subMap(from, true, to, true));
  }

  @Override
  public KeyValueIterator<String, V> reverseRange(String from, String to) {
    if (from.compareTo(to) > 0) {
      throw new IllegalArgumentException("Range from " + from + " is after its end " + to);
    }
    return iterator(entries.subMap(from, true, to, true).descendingMap());
  }

  @Override
  public KeyValueIterator<String, V> all() {
    return iterator(entries);
  }

  @Override
  public long approximateNumEntries() {
    return entries.size();
  }

  private static <V> KeyValueIterator<String, V> iterator(Map<String, V> map) {
    Iterator<Map.Entry<String, V>> iterator = new java.util.ArrayList<>(map.entrySet()).iterator(); // in the map's order
    return new KeyValueIterator<>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public KeyValue<String, V> next() {
        Map.Entry<String, V> entry = iterator.next();
        return KeyValue.pair(entry.getKey(), entry.getValue());
      }

      @Override
      public String peekNextKey() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
      }
    };
  }
}
