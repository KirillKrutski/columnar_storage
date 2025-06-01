use super::{storage::Column, cache::HybridCache};
use crate::ColumnBuilder;
use crossbeam::channel::{bounded, Sender};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration
};

pub struct Prefetcher {
    sender: Sender<String>,
}

impl Prefetcher {
    pub fn new(column: Arc<Column>, cache: Arc<Mutex<HybridCache>>) -> Self {
        let (sender, receiver) = bounded::<String>(10);

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
    use tempfile::NamedTempFile;

    #[test]
    fn test_prefetch_mechanism() {
        // Создаем тестовую колонку
        let data = vec![1i32, 2, 3];
        let bytes: Vec<u8> = data.iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();
        
        let column = ColumnBuilder::new("test_col".to_string(), bytes)
            .build(NamedTempFile::new().unwrap().path())
            .unwrap();
        
        let column = Arc::new(column);
        let cache = Arc::new(Mutex::new(HybridCache::new(100)));
        
        let prefetcher = Prefetcher::new(column.clone(), cache.clone());
        
        // Запускаем предзагрузку
        prefetcher.schedule_prefetch("test_col".to_string());
        
        // Даем время на обработку
        thread::sleep(Duration::from_millis(50));
        
        // Проверяем, что данные появились в кэше
        assert!(cache.lock().unwrap().get("test_col").is_some());
    }
}