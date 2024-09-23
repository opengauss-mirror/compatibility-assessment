/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.replay.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * LRU-k Cache
 *
 * @param <K> K
 * @param <V> V
 * @since 2024-07-01
 */
public class LrukCache<K, V> {
    private final int capacity;
    private final int k;
    private final Map<K, V> cache;
    private final Map<K, LinkedList<Long>> accessHistory;

    /**
     * constructor
     *
     * @param capacity cache capacity
     * @param k k of LRU-k
     */
    public LrukCache(int capacity, int k) {
        this.capacity = capacity;
        this.k = k;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true);
        this.accessHistory = new HashMap<>();
    }

    /**
     * get value from cache
     *
     * @param key key
     * @return value
     */
    public Optional<V> get(K key) {
        if (cache.containsKey(key)) {
            recordAccess(key);
            return Optional.of(cache.get(key));
        }
        return Optional.empty();
    }

    /**
     * add or update value to cache
     *
     * @param key key
     * @param value value
     */
    public void put(K key, V value) {
        if (!cache.containsKey(key) && cache.size() >= capacity) {
            evict();
        }
        cache.put(key, value);
        recordAccess(key);
    }

    private void recordAccess(K key) {
        accessHistory.putIfAbsent(key, new LinkedList<>());
        LinkedList<Long> accesses = accessHistory.get(key);
        accesses.add(System.nanoTime());
        if (accesses.size() > k) {
            accesses.removeFirst();
        }
    }

    private void evict() {
        K evictKey = null;
        long oldestKthAccessTime = Long.MAX_VALUE;
        for (Map.Entry<K, LinkedList<Long>> entry : accessHistory.entrySet()) {
            LinkedList<Long> accesses = entry.getValue();
            if (accesses.size() == k) {
                long kthAccessTime = accesses.getFirst();
                if (kthAccessTime < oldestKthAccessTime) {
                    oldestKthAccessTime = kthAccessTime;
                    evictKey = entry.getKey();
                }
            }
        }
        if (evictKey == null) {
            evictKey = cache.keySet().iterator().next();
        }
        cache.remove(evictKey);
        accessHistory.remove(evictKey);
    }
}
