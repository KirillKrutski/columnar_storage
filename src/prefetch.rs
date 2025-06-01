use super::storage::Column;
use crate::cache::HybridCache;
use crossbeam::channel::{bounded, Sender};
use std::{
    sync::{Arc, Mutex},
    thread
};

pub struct Prefetcher {
    sender: Sender<String>,
}

impl Prefetcher {
    pub fn new(column: Arc<Column>, cache: Arc<Mutex<HybridCache>>) -> Self {
        let (sender, receiver) = bounded(10);

        thread::spawn(move || {
            while let Ok(col_name) = receiver.recv() {
                if cache.lock().unwrap().get(&col_name).is_none() {
                    if let Ok(data) = column.decompress_parallel() {
                        cache.lock().unwrap().insert(col_name, Arc::new(data));
                    }
                }
            }
        });

        Self { sender }
    }

    pub fn schedule_prefetch(&self, column_name: String) {
        let _ = self.sender.send(column_name);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_prefetch_mechanism() {
        let test_column = Arc::new(MemColumn::new());
        let cache = Arc::new(Mutex::new(HybridCache::new(100)));
        
        let prefetcher = Prefetcher::new(test_column.clone(), cache.clone());
        
        // Запуск предзагрузки
        prefetcher.schedule_prefetch("test_col".to_string());
        
        // Даем время на обработку
        thread::sleep(Duration::from_millis(50));
        
        // Проверяем, что данные появились в кэше
        assert!(cache.lock().unwrap().get("test_col").is_some());
    }
}