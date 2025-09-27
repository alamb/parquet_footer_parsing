mod benchmark;
mod datagen;
mod file_type;
mod parquet_file;

use crate::benchmark::{MetadataParseBenchmark, MetadataParseResult};
use crate::file_type::FileType;
use crate::parquet_file::ParquetFileSpecBuilder;
use comfy_table::Table;
use std::fs;
use std::path::PathBuf;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let output_dir = PathBuf::from("output");
    fs::create_dir_all(&output_dir).unwrap();

    let mut specs = vec![];
    for file_type in [FileType::Float, FileType::String] {
    //for file_type in [FileType::String] {
        //for columns in [100, 1000, 10000] {
        for columns in [100, 1000, 10000, 100000] {
            specs.push(
                ParquetFileSpecBuilder::new()
                    .with_path(output_dir.join(format!("{file_type}_data_{columns}_cols.parquet")))
                    .with_file_type(file_type)
                    .with_columns(columns)
                    .with_row_groups(20)
                    // don't need many rows per row group to test metadata parsing
                    .with_rows_per_row_group(1_000)
                    .build(),
            );
        }
    }

    println!("Creating parquet files in {:?}", output_dir);
    for spec in &specs {
        spec.create().await;
    }
    println!("Done creating parquet files");

    let mut results = vec![];
    for spec in &specs {
        let description = spec.to_string();
        println!("running benchmark on {description}");
        let benchmark = MetadataParseBenchmark::new(description, spec.path().clone());
        let result = benchmark.run();
        println!("{result}");
        results.push(result);
    }

    // make a table of results
    let mut table = Table::new();
    MetadataParseResult::set_headers(&mut table);
    for result in &results {
        result.add_to_table(&mut table);
    }
    println!("Summary of results:");
    println!("{table}");

    println!("CSV output:");
    println!("{}", MetadataParseResult::csv_headers().join(",") );
    for result in &results {
        println!("{}", result.to_csv_row().join(",") );
    }

}
