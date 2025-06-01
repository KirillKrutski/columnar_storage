use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

pub struct HybridCache {
    lfu: lfu_cache::LfuCache<String, Arc<Vec<u8>>>,
    lru: lru::LruCache<String, Arc<Vec<u8>>>,
    lfu_keys: HashSet<String>,
    access_stats: HashMap<String, (u64, Instant)>,
    size: usize,
}

impl HybridCache {
    pub fn new(size: usize) -> Self {
        Self {
            lfu: lfu_cache::LfuCache::with_capacity(size / 2),
            lru: lru::LruCache::new(std::num::NonZeroUsize::new(size / 2).unwrap()),
            lfu_keys: HashSet::new(),
            access_stats: HashMap::new(),
            size,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<Arc<Vec<u8>>> {
        let key_str = key.to_string();
        let entry = self.access_stats.entry(key_str.clone()).or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();

        if let Some(val) = self.lfu.get(&key_str) {
            Some(val.clone())
        } else {
            self.lru.get(&key_str).cloned()
        }
    }

    pub fn insert(&mut self, key: String, value: Arc<Vec<u8>>) {
    let entry = self.access_stats.entry(key.clone()).or_insert((0, Instant::now()));
    entry.0 += 1;
    entry.1 = Instant::now();

    if entry.0 > 5 {
        self.lfu.insert(key.clone(), value);
        self.lfu_keys.insert(key);
    } else {
        self.lru.put(key, value);
    }

    self.rebalance();
}


    fn rebalance(&mut self) {
        while self.lfu.len() + self.lru.len() > self.size {
            if self.lfu.len() > self.size / 2 {
                if let Some(key_to_remove) = self.least_used_key_in_lfu() {
                    self.lfu.remove(&key_to_remove);
                    self.lfu_keys.remove(&key_to_remove);
                    self.access_stats.remove(&key_to_remove);
                } else {
                    break;
                }
            } else {
                if let Some((key, _)) = self.lru.pop_lru() {
                    self.access_stats.remove(&key);
                }
            }
        }
    }

    fn least_used_key_in_lfu(&self) -> Option<String> {
        self.lfu_keys
            .iter()
            .min_by_key(|key| {
                self.access_stats
                    .get(*key)
                    .map(|(freq, time)| (*freq, *time))
                    .unwrap_or((0, Instant::now()))
            })
            .cloned()
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
        
        // Добавляем часто используемый элемент (6 раз)
        for _ in 0..6 {
            cache.insert("frequent".to_string(), test_data.clone());
            cache.get("frequent"); // Увеличиваем счетчик обращений
        }
        
        // Добавляем редко используемый элемент (1 раз)
        cache.insert("recent".to_string(), test_data.clone());
        
        // Проверяем, что частый элемент остался в LFU
        assert!(cache.get("frequent").is_some(), "Частый элемент должен остаться в LFU");
        
        // Проверяем, что редкий элемент остался в LRU
        assert!(cache.get("recent").is_some(), "Редкий элемент должен быть в LRU");
        
        // Проверяем вытеснение - добавляем много элементов
        for i in 0..15 {
            cache.insert(format!("item_{}", i), test_data.clone());
        }
        
        // Частый элемент должен остаться
        assert!(cache.get("frequent").is_some(), "Частый элемент не должен вытесняться");
        
        // Редкий элемент мог вытесниться
        println!("Cache state: {:?}", cache.access_stats);
    }
}
