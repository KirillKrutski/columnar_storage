use std::{
    sync::{Arc, Mutex},
    collections::HashMap,
    time::Instant
};

pub struct HybridCache {
    lfu: lfu_cache::LfuCache<String, Arc<Vec<u8>>>,
    lru: lru::LruCache<String, Arc<Vec<u8>>>,
    access_stats: HashMap<String, (u64, Instant)>,
    size: usize,
}

impl HybridCache {
    pub fn new(size: usize) -> Self {
        Self {
            lfu: lfu_cache::LfuCache::with_capacity(size / 2),
            lru: lru::LruCache::new(std::num::NonZeroUsize::new(size / 2).unwrap()),
            access_stats: HashMap::new(),
            size,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Arc<Vec<u8>>> {
        let entry = self.access_stats.entry(key.to_string())
            .or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();

        if let Some(val) = self.lfu.get(&key.to_string()) {
            Some(val.clone())
        } else {
            self.lru.get(&key.to_string()).cloned()
        }
    }

    pub fn insert(&mut self, key: String, value: Arc<Vec<u8>>) {
        let (freq, _) = *self.access_stats.get(&key).unwrap_or(&(0, Instant::now()));
        
        if freq > 5 {
            self.lfu.insert(key, value);
        } else {
            self.lru.put(key, value);
        }
        
        self.rebalance();
    }

    fn rebalance(&mut self) {
        while self.lfu.len() + self.lru.len() > self.size {
            if self.lfu.len() > self.size / 2 {
                self.lfu.pop();
            } else {
                self.lru.pop_lru();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_hybrid_cache_behavior() {
        let mut cache = HybridCache::new(10);
        let test_data = Arc::new(vec![1u8, 2, 3, 4]);
        
        // Тестирование LFU-логики
        for _ in 0..6 {
            cache.insert("frequent".to_string(), test_data.clone());
            cache.get("frequent");
        }
        
        // Тестирование LRU-логики
        cache.insert("recent".to_string(), test_data.clone());
        
        assert!(cache.get("frequent").is_some()); // Должно быть в LFU
        assert!(cache.get("recent").is_some());   // Должно быть в LRU
        
        // Тестирование вытеснения
        for i in 0..20 {
            cache.insert(format!("item_{}", i), test_data.clone());
        }
        
        assert!(cache.get("frequent").is_some()); // Частые должны остаться
        assert!(cache.get("recent").is_none());   // Недавние могли вытесниться
    }
}