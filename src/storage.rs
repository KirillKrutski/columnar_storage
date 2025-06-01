use std::{fs::File, path::Path, sync::Arc};
use memmap2::Mmap;
use bloomfilter::Bloom;
use zstd::{encode_all as zstd_compress, decode_all as zstd_decompress};
use rayon::prelude::*;

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub mmap: Arc<Mmap>,
    pub min: i32,
    pub max: i32,
    pub is_compressed: bool,
    pub bloom_filter: Bloom<i32>,
}

pub struct ColumnBuilder {
    name: String,
    data: Vec<u8>,
    min: i32,
    max: i32,
    is_compressed: bool,
}

impl ColumnBuilder {
    pub fn new(name: String, data: Vec<u8>) -> Self {
        let (min, max) = Self::compute_stats(&data);
        Self { name, data, min, max, is_compressed: false }
    }

    pub fn compress(&mut self) -> std::io::Result<()> {
        if !self.is_compressed {
            self.data = zstd_compress(&*self.data, 3)?;
            self.is_compressed = true;
        }
        Ok(())
    }

    pub fn build(self, path: &Path) -> std::io::Result<Column> {
        std::fs::write(path, &self.data)?;
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        let mut bloom = Bloom::new_for_fp_rate(1000, 0.01);
        for chunk in self.data.chunks_exact(4) {
            let value = i32::from_le_bytes(chunk.try_into().unwrap());
            bloom.set(&value);
        }
        
        Ok(Column {
            name: self.name,
            mmap: Arc::new(mmap),
            min: self.min,
            max: self.max,
            is_compressed: self.is_compressed,
            bloom_filter: bloom,
        })
    }

    fn compute_stats(data: &[u8]) -> (i32, i32) {
        let mut min = i32::MAX;
        let mut max = i32::MIN;
        for chunk in data.chunks_exact(4) {
            let value = i32::from_le_bytes(chunk.try_into().unwrap());
            min = min.min(value);
            max = max.max(value);
        }
        (min, max)
    }
}

impl Column {
    pub fn decompress_parallel(&self) -> std::io::Result<Vec<u8>> {
        if !self.is_compressed {
            return Ok(self.mmap[..].to_vec());
        }

        const CHUNK_SIZE: usize = 1024 * 1024;
        let compressed_data = &self.mmap[..];
        
        if compressed_data.len() <= CHUNK_SIZE {
            return zstd_decompress(compressed_data);
        }

        let chunks: Vec<_> = compressed_data.chunks(CHUNK_SIZE).collect();
        let decompressed_chunks: Vec<Vec<u8>> = chunks
            .into_par_iter()
            .map(|chunk| zstd_decompress(chunk).unwrap())
            .collect();
        
        let mut result = Vec::with_capacity(compressed_data.len() * 2);
        for chunk in decompressed_chunks {
            result.extend(chunk);
        }
        
        Ok(result)
    }

    pub fn get_value(&self, idx: usize) -> Option<i32> {
        let offset = idx * 4;
        if offset + 4 > self.mmap.len() {
            return None;
        }
        Some(i32::from_le_bytes(
            self.mmap[offset..offset+4].try_into().unwrap()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_column_creation() {
        // Подготовка тестовых данных
        let test_data = vec![10i32, 20, 30];
        let bytes: Vec<u8> = test_data.iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();

        // Создание временного файла
        let tmp_file = NamedTempFile::new().unwrap();
        
        // Тестирование билдера
        let mut builder = ColumnBuilder::new("test_col".to_string(), bytes.clone());
        assert!(!builder.is_compressed);
        
        // Тестирование сжатия
        builder.compress().unwrap();
        assert!(builder.is_compressed);

        // Тестирование финализации
        let column = builder.build(tmp_file.path()).unwrap();
        assert_eq!(column.name, "test_col");
        assert_eq!(column.min, 10);
        assert_eq!(column.max, 30);
    }

    #[test]
    fn test_value_access() {
        let data = vec![100i32, 200, 300];
        let bytes: Vec<u8> = data.iter().flat_map(|x| x.to_le_bytes()).collect();
        
        let column = ColumnBuilder::new("test".to_string(), bytes)
            .build(NamedTempFile::new().unwrap().path())
            .unwrap();
            
        assert_eq!(column.get_value(0), Some(100));
        assert_eq!(column.get_value(2), Some(300));
        assert_eq!(column.get_value(3), None); // Проверка выхода за границы
    }
}