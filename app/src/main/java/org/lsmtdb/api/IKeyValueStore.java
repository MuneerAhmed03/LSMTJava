package org.lsmtdb.api;

import java.io.IOException;

public interface IKeyValueStore {
    public void put(String key, Object value) throws IOException;
    public Object get(String key) throws IOException;
    public void delete(String key) throws IOException;
};