use std::path::PathBuf;

fn main() {

    let output_dir = PathBuf::from("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    ParquetFileSpec {
        path: output_dir.join("float_data.parquet"),
        file_type: FileType::Float,
        columns: 100,
        row_groups: 20,
        rows_per_row_group: 1_000_000,
    }.create();

}


/// The type of file to create
#[derive(Debug)]
enum FileType {
    Float, // float32 columns
    String, // utf8 string columns
}

/// Creates a parquet files with specified characteristics
#[derive(Debug)]
struct ParquetFileSpec {
    path: PathBuf,
    file_type: FileType,
    columns: usize,
    row_groups: usize,
    rows_per_row_group: usize,
}

impl ParquetFileSpec {

    fn create(self) {
        // Implementation to create a parquet file based on the spec
        println!("Creating a {self:#?}");

    }
}