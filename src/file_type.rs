use crate::datagen;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Float32Type, Schema};
use std::fmt::Display;
use std::sync::Arc;

/// The type of file to create
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Float,  // float32 columns
    String, // utf8 string columns
}
impl Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Float => write!(f, "Float32"),
            FileType::String => write!(f, "String"),
        }
    }
}

impl FileType {
    pub fn create_batch(&self, seed: usize, num_rows: usize, columns: usize) -> RecordBatch {
        let null_density = 0.0001;
        let mut arrays: Vec<ArrayRef> = vec![];
        match self {
            FileType::Float => {
                for i in 0..columns {
                    let array_seed = seed * columns + i;
                    let array = datagen::create_primitive_array_with_seed::<Float32Type>(
                        num_rows,
                        null_density,
                        array_seed as u64,
                    );
                    arrays.push(Arc::new(array));
                }
            }
            FileType::String => {
                let max_str_len = 20;
                for i in 0..columns {
                    let array_seed = seed * columns + i;
                    let array = datagen::create_string_array_with_max_len::<i32>(
                        num_rows,
                        null_density,
                        max_str_len,
                        array_seed as u64,
                    );
                    arrays.push(Arc::new(array));
                }
            }
        }
        RecordBatch::try_new(self.schema(columns), arrays).unwrap()
    }

    pub fn schema(&self, columns: usize) -> Arc<Schema> {
        match self {
            FileType::Float => {
                let fields: Vec<Field> = (0..columns)
                    .map(|i| Field::new(format!("col_{i}"), DataType::Float32, true))
                    .collect();
                Arc::new(Schema::new(fields))
            }
            FileType::String => {
                let fields: Vec<Field> = (0..columns)
                    .map(|i| Field::new(format!("col_{i}"), DataType::Utf8, true))
                    .collect();
                Arc::new(Schema::new(fields))
            }
        }
    }
}
