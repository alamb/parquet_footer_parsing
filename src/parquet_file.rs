use crate::file_type::FileType;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use parquet_56::arrow::ArrowWriter;
use parquet_56::arrow::arrow_writer::{ArrowColumnChunk, ArrowColumnWriter, compute_leaves};
use parquet_56::file::properties::WriterProperties;
use std::fmt::Display;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Builder for a ParquetFileSpec
#[derive(Debug, Default)]
pub struct ParquetFileSpecBuilder {
    path: Option<PathBuf>,
    file_type: Option<FileType>,
    columns: Option<usize>,
    row_groups: Option<usize>,
    rows_per_row_group: Option<usize>,
}

impl ParquetFileSpecBuilder {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }
    pub fn with_file_type(mut self, file_type: FileType) -> Self {
        self.file_type = Some(file_type);
        self
    }
    pub fn with_columns(mut self, columns: usize) -> Self {
        self.columns = Some(columns);
        self
    }
    pub fn with_row_groups(mut self, row_groups: usize) -> Self {
        self.row_groups = Some(row_groups);
        self
    }
    pub fn with_rows_per_row_group(mut self, rows_per_row_group: usize) -> Self {
        self.rows_per_row_group = Some(rows_per_row_group);
        self
    }
    pub fn build(self) -> ParquetFileSpec {
        let Self {
            path,
            file_type,
            columns,
            row_groups,
            rows_per_row_group,
        } = self;

        ParquetFileSpec {
            path: path.expect("path is required"),
            file_type: file_type.expect("file_type is required"),
            columns: columns.expect("columns is required"),
            row_groups: row_groups.expect("row_groups is required"),
            rows_per_row_group: rows_per_row_group.expect("rows_per_row_group is required"),
        }
    }
}

/// Creates a parquet files with specified characteristics
#[derive(Debug)]
pub struct ParquetFileSpec {
    path: PathBuf,
    file_type: FileType,
    columns: usize,
    row_groups: usize,
    rows_per_row_group: usize,
}

impl Display for ParquetFileSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            " {:?} {} cols {} row groups",
            self.file_type, self.columns, self.row_groups
        )
    }
}

impl ParquetFileSpec {
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub async fn create(&self) {
        if fs::exists(&self.path).unwrap() {
            println!("File {:?} already exists, skipping", self.path);
            return;
        };

        // Implementation to create a parquet file based on the spec
        println!("Creating a {self:#?}");

        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        let schema = self.file_type.schema(self.columns);
        let props = WriterProperties::builder()
            .set_max_row_group_size(self.rows_per_row_group)
            .build();
        let writer = ArrowWriter::try_new(&mut temp_file, schema, Some(props)).unwrap();

        let (mut file_writer, row_group_factory) = writer.into_serialized_writer().unwrap();

        let mut rows_written = 0;
        for rg in 0..self.row_groups {
            let col_writers = row_group_factory.create_column_writers(rg).unwrap();
            let row_group_encoder =
                RowGroupEncoder::new(self.file_type.schema(self.columns), col_writers);

            let encoded_columns = encode_row_group(
                self.file_type,
                self.columns,
                self.rows_per_row_group,
                row_group_encoder,
            )
            .await;
            let mut rg_writer = file_writer.next_row_group().unwrap();
            for col_chunk in encoded_columns.into_iter() {
                col_chunk.append_to_row_group(&mut rg_writer).unwrap();
            }
            let rg_metadata = rg_writer.close().unwrap();
            let mb_size = rg_metadata.total_byte_size() as f64 / (1024.0 * 1024.0);
            println!(
                "Completed row group of {} columns, {} rows {mb_size:.02} MB",
                rg_metadata.num_columns(),
                rg_metadata.num_rows(),
            );
            rows_written += rg_metadata.num_rows();
        }
        file_writer.close().unwrap();
        println!("Wrote {rows_written} rows to {:?}", self.path);

        // rename the temp file to the final path
        temp_file.persist(&self.path).unwrap();
    }
}

async fn encode_row_group(
    file_type: FileType,
    columns: usize,
    rows_per_row_group: usize,
    mut row_group_encoder: RowGroupEncoder,
) -> Vec<ArrowColumnChunk> {
    let num_rows = rows_per_row_group.min(100);
    // use the same batch repeatedly otherwise the data generation will dominate the time
    let batch = file_type.create_batch(0, num_rows, columns);
    let mut rows_written = 0;
    while rows_written < rows_per_row_group {
        let rows_left = rows_per_row_group - rows_written;
        let batch = if rows_left < batch.num_rows() {
            batch.slice(0, rows_left)
        } else {
            batch.clone()
        };
        // todo handle the case where rows is not a multiple of batch size
        row_group_encoder.encode_batch(&batch).await;
        rows_written += batch.num_rows();
    }
    row_group_encoder.close().await
}

struct RowGroupEncoder {
    // tasks for encoding column chunks. Return (column_index, column_chunk)
    join_set: JoinSet<(usize, ArrowColumnChunk)>,
    /// channel to send arrays to column writers
    writer_txs: Vec<tokio::sync::mpsc::Sender<ArrayRef>>,
}

impl RowGroupEncoder {
    fn new(schema: SchemaRef, column_writers: Vec<ArrowColumnWriter>) -> Self {
        // setup the channel and tasks
        let mut join_set = JoinSet::new();
        let mut writer_txs = Vec::with_capacity(column_writers.len());
        for (column_index, mut writer) in column_writers.into_iter().enumerate() {
            let buffer_size = 2;
            let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(buffer_size);
            let field = Arc::clone(&schema.fields()[column_index]);
            let writer_task = async move {
                // receive arrays and write them
                while let Some(array) = writer_rx.recv().await {
                    let leaves = compute_leaves(&field, &array).unwrap();
                    for leaf in leaves {
                        writer.write(&leaf).unwrap();
                    }
                }
                // when no more arrays, close the writer and return the column chunk
                let arrow_column_chunk = writer.close().unwrap();
                (column_index, arrow_column_chunk)
            };
            join_set.spawn(writer_task);
            writer_txs.push(writer_tx);
        }

        Self {
            join_set,
            writer_txs,
        }
    }

    /// Encode the next batch, sending arrays to column writers on separate tasks to encode
    /// in parallel.
    async fn encode_batch(&mut self, batch: &RecordBatch) {
        for (array, tx) in batch.columns().iter().zip(self.writer_txs.iter()) {
            let array = array.clone();
            // send will fail if the receiver has been dropped
            let tx = tx.clone();
            if let Err(e) = tx.send(array).await {
                eprintln!("Failed to send array to writer: {}", e);
                return;
            }
        }
    }

    // Completes the row group and returns the column chunks
    async fn close(self) -> Vec<ArrowColumnChunk> {
        // close all the senders to signal no more arrays
        drop(self.writer_txs);
        // collect all the column chunks from the tasks
        // (note need to reorder this correctly)
        let mut completed_columns = self.join_set.join_all().await;
        // sort by column index
        completed_columns.sort_by_key(|res| res.0);
        // extract the column chunks
        completed_columns.into_iter().map(|res| res.1).collect()
    }
}
