package example.myproject.datatypes;

import peersim.config.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

public class MessageMap<K, V> extends LinkedHashMap<K, V> {

    private static final int delta;

    static {
        delta = Configuration.getInt("delta");
    }

    // TODO verificar se isto está bem
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
        return this.size() > (delta * 2);
    }
}