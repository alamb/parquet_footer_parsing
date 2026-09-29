use bytes::Bytes;
use comfy_table::Table;
use std::fmt::{Debug, Display};
use std::fs;
use std::io::{Read, Seek};
use std::ops::Range;
use std::path::PathBuf;
use std::time::Duration;

/// metadata parsing benchmark function
/// Given a filename:
/// Loads the metadata and page indexes into memory
/// Then times how long it takes to parse the metadata and page indexes
pub struct MetadataParseBenchmark {
    description: String,
    /// number of times to parse the footer
    num_runs: usize,
    /// path to the parquet file
    file_path: PathBuf,
    file_len: u64,
    footer_range: Range<u64>,
    footer_bytes: Bytes,
    metadata_range: Range<u64>,
    metadata_bytes: Bytes,
    index_range: Range<u64>,
    index_bytes: Bytes,
}
impl Debug for MetadataParseBenchmark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataParseBenchmark")
            .field("file_path", &self.file_path)
            .field("file_len", &self.file_len)
            .field("footer_bytes", &self.footer_bytes.len())
            .field("metadata_bytes", &self.metadata_bytes.len())
            .field("index_bytes", &self.index_bytes.len())
            .finish()
    }
}

/// Macro that pushes the specified byte range and bytes to the decoder
macro_rules! push_range
{
    ($decoder:expr, $range:expr, $bytes:expr) => {
        $decoder
            .push_ranges(vec![$range.clone()], vec![$bytes.clone()])
            .unwrap();
    };
}


// reads the specified byte range from the cursor into a contiguous memory region
// (Bytes object)
fn read_byte_range(cursor: &mut (impl Read + Seek), range: &Range<u64>) -> Bytes {
    let length = (range.end - range.start) as usize;
    cursor.seek(std::io::SeekFrom::Start(range.start)).unwrap();
    let mut buffer = vec![0u8; length];
    cursor.read_exact(&mut buffer).unwrap();
    Bytes::from(buffer)
}


impl MetadataParseBenchmark {
    pub fn new(description: impl Into<String>, file_path: PathBuf) -> Self {
        use parquet_56::file::metadata::ParquetMetaDataPushDecoder;
        use parquet_56::DecodeResult;

        let mut file = fs::File::open(&file_path).unwrap();
        let file_len = fs::metadata(&file_path).unwrap().len();
        let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();

        // decode the metadata once to get the locations
        let DecodeResult::NeedsData(mut ranges) = decoder.try_decode().unwrap() else {
            panic!("Needs expected footer bytes");
        };
        assert_eq!(ranges.len(), 1);
        let footer_range = ranges.pop().unwrap();
        let footer_bytes = read_byte_range(&mut file, &footer_range);
        push_range!(&mut decoder, &footer_range, &footer_bytes);

        let DecodeResult::NeedsData(mut ranges) = decoder.try_decode().unwrap() else {
            panic!("Needs expected metadata bytes");
        };
        assert_eq!(ranges.len(), 1);
        let metadata_range = ranges.pop().unwrap();
        let metadata_bytes = read_byte_range(&mut file, &metadata_range);
        push_range!(&mut decoder, &metadata_range, &metadata_bytes);

        // now read index range
        let DecodeResult::NeedsData(mut ranges) = decoder.try_decode().unwrap() else {
            panic!("Needs expected index bytes");
        };
        // should be one index range
        assert_eq!(ranges.len(), 1);
        let index_range = ranges.pop().unwrap();
        let index_bytes = read_byte_range(&mut file, &index_range);
        push_range!(&mut decoder, &index_range, &index_bytes);

        let DecodeResult::Data(_metadata) = decoder.try_decode().unwrap() else {
            panic!("Expected to be done with parsing");
        };

        // TODO: make num_runs configurable
        let num_runs = 10;
        //let num_runs = 100;
        Self {
            description: description.into(),
            num_runs,
            file_path,
            file_len,
            footer_range,
            footer_bytes,
            metadata_range,
            metadata_bytes,
            index_range,
            index_bytes,
        }
    }

    /// Released arrow 56 uses the thrift compiler to generate the parquet metadata structs
    fn run_arrow_56(&self) -> Timing {
        use parquet_56::file::metadata::ParquetMetaDataPushDecoder;
        use parquet_56::DecodeResult;
        println!("Arrow 56 (using thrift compiler)...");

        // parse the metadata and index once, returning the time taken
        // for each (metadata, index)
        let run_once = || {
            let mut decoder = ParquetMetaDataPushDecoder::try_new(self.file_len).unwrap();
            push_range!(&mut decoder, &self.footer_range, &self.footer_bytes);
            push_range!(&mut decoder, &self.metadata_range, &self.metadata_bytes);
            // this will now parse the metadata
            let start = std::time::Instant::now();
            let DecodeResult::NeedsData(_range) = decoder.try_decode().unwrap() else {
                panic!("Needs expected index bytes");
            };
            let metadata_parsing_duration = start.elapsed();
            let start = std::time::Instant::now();
            push_range!(&mut decoder, &self.index_range, &self.index_bytes);
            // this is the index parsing
            let DecodeResult::Data(_metadata) = decoder.try_decode().unwrap() else {
                panic!("Expected to be done with parsing");
            };
            let index_parsing_duration = start.elapsed();
            (metadata_parsing_duration, index_parsing_duration)
        } ;

        // warm up with 10 runs
        for _ in 0..10 {
            run_once();
        }

        // now run the actual benchmark
        let mut metadata_parsing_duration = Duration::from_secs(0);
        let mut index_parsing_duration = Duration::from_secs(0);
        for _ in 0..self.num_runs {
            let (md_duration, idx_duration) = run_once();
            metadata_parsing_duration += md_duration;
            index_parsing_duration += idx_duration;
        }
        Timing {
            num_runs: self.num_runs,
            metadata_parsing_duration,
            index_parsing_duration,
        }
    }

    /// Arrow 57 uses a custom thrift decoder for parquet metadata
    fn run_arrow_57(&self) -> Timing {
        use parquet_57::file::metadata::ParquetMetaDataPushDecoder;
        use parquet_57::DecodeResult;
        println!("Arrow 57 (custom thrift decoder)...");

        // parse the metadata and index once, returning the time taken
        // for each (metadata, index)
        let run_once = || {
            let mut decoder = ParquetMetaDataPushDecoder::try_new(self.file_len).unwrap();
            push_range!(&mut decoder, &self.footer_range, &self.footer_bytes);
            push_range!(&mut decoder, &self.metadata_range, &self.metadata_bytes);
            // this will now parse the metadata
            let start = std::time::Instant::now();
            let DecodeResult::NeedsData(_range) = decoder.try_decode().unwrap() else {
                panic!("Needs expected index bytes");
            };
            let metadata_parsing_duration = start.elapsed();
            let start = std::time::Instant::now();
            push_range!(&mut decoder, &self.index_range, &self.index_bytes);
            // this is the index parsing
            let DecodeResult::Data(_metadata) = decoder.try_decode().unwrap() else {
                panic!("Expected to be done with parsing");
            };
            let index_parsing_duration = start.elapsed();
            (metadata_parsing_duration, index_parsing_duration)
        } ;

        // warm up with 10 runs
        for _ in 0..10 {
            run_once();
        }

        // now run the actual benchmark
        let mut metadata_parsing_duration = Duration::from_secs(0);
        let mut index_parsing_duration = Duration::from_secs(0);
        for _ in 0..self.num_runs {
            let (md_duration, idx_duration) = run_once();
            metadata_parsing_duration += md_duration;
            index_parsing_duration += idx_duration;
        }
        Timing {
            num_runs: self.num_runs,
            metadata_parsing_duration,
            index_parsing_duration,
        }
    }


    /// Unmodified arrow-rs main branch (unreleased, will become 60.0.0), used as an
    /// up to date baseline for comparison
    fn run_arrow_60(&self) -> Timing {
        use parquet_60::file::metadata::ParquetMetaDataPushDecoder;
        use parquet_60::DecodeResult;
        println!("Arrow 60 (unmodified)...");

        // parse the metadata and index once, returning the time taken
        // for each (metadata, index)
        let run_once = || {
            let mut decoder = ParquetMetaDataPushDecoder::try_new(self.file_len).unwrap();
            push_range!(&mut decoder, &self.footer_range, &self.footer_bytes);
            push_range!(&mut decoder, &self.metadata_range, &self.metadata_bytes);
            // this will now parse the metadata
            let start = std::time::Instant::now();
            let DecodeResult::NeedsData(_range) = decoder.try_decode().unwrap() else {
                panic!("Needs expected index bytes");
            };
            let metadata_parsing_duration = start.elapsed();
            let start = std::time::Instant::now();
            push_range!(&mut decoder, &self.index_range, &self.index_bytes);
            // this is the index parsing
            let DecodeResult::Data(_metadata) = decoder.try_decode().unwrap() else {
                panic!("Expected to be done with parsing");
            };
            let index_parsing_duration = start.elapsed();
            (metadata_parsing_duration, index_parsing_duration)
        } ;

        // warm up with 10 runs
        for _ in 0..10 {
            run_once();
        }

        // now run the actual benchmark
        let mut metadata_parsing_duration = Duration::from_secs(0);
        let mut index_parsing_duration = Duration::from_secs(0);
        for _ in 0..self.num_runs {
            let (md_duration, idx_duration) = run_once();
            metadata_parsing_duration += md_duration;
            index_parsing_duration += idx_duration;
        }
        Timing {
            num_runs: self.num_runs,
            metadata_parsing_duration,
            index_parsing_duration,
        }
    }

    /// Version of parquet with support for `ColumnChunkMask`
    /// (https://github.com/apache/arrow-rs/pull/11157), used to select only a single
    /// column when decoding the PageIndex (ColumnIndex / OffsetIndex), rather than
    /// decoding the PageIndex for all columns.
    fn run_page_index_selection(&self) -> Timing {
        use parquet_page_index_selection::file::metadata::{
            ColumnChunkMask, ParquetMetaDataPushDecoder,
        };
        use parquet_page_index_selection::DecodeResult;
        println!("Page Index Selection (single column)...");

        // parse the metadata and index once, returning the time taken
        // for each (metadata, index)
        let run_once = || {
            // only decode the ColumnIndex / OffsetIndex for column 0
            let mut decoder = ParquetMetaDataPushDecoder::try_new(self.file_len)
                .unwrap()
                .with_column_index_mask(ColumnChunkMask::columns([0]))
                .with_offset_index_mask(ColumnChunkMask::columns([0]));
            push_range!(&mut decoder, &self.footer_range, &self.footer_bytes);
            push_range!(&mut decoder, &self.metadata_range, &self.metadata_bytes);
            // this will now parse the metadata
            let start = std::time::Instant::now();
            let DecodeResult::NeedsData(_range) = decoder.try_decode().unwrap() else {
                panic!("Needs expected index bytes");
            };
            let metadata_parsing_duration = start.elapsed();
            let start = std::time::Instant::now();
            push_range!(&mut decoder, &self.index_range, &self.index_bytes);
            // this is the index parsing (only for the selected column)
            let DecodeResult::Data(_metadata) = decoder.try_decode().unwrap() else {
                panic!("Expected to be done with parsing");
            };
            let index_parsing_duration = start.elapsed();
            (metadata_parsing_duration, index_parsing_duration)
        } ;


        // warm up with 10 runs
        for _ in 0..10 {
            run_once();
        }

        // now run the actual benchmark
        let mut metadata_parsing_duration = Duration::from_secs(0);
        let mut index_parsing_duration = Duration::from_secs(0);
        for _ in 0..self.num_runs {
            let (md_duration, idx_duration) = run_once();
            metadata_parsing_duration += md_duration;
            index_parsing_duration += idx_duration;
        }
        Timing {
            num_runs: self.num_runs,
            metadata_parsing_duration,
            index_parsing_duration,
        }
    }


    pub fn run(&self) -> MetadataParseResult {
        print!("Running metadata parse benchmark on {self:#?} ... ");

        MetadataParseResult {
            description: self.description.clone(),
            arrow_56_timing: self.run_arrow_56(),
            arrow_57_timing: self.run_arrow_57(),
            arrow_60_timing: self.run_arrow_60(),
            page_index_selection_timing: self.run_page_index_selection(),
        }
    }
}

#[derive(Debug)]
pub struct Timing {
    /// Total number of times the parsing was run
    num_runs: usize,
    /// Total duration for parsing the metadata (excluding index parsing)
    metadata_parsing_duration: Duration,
    /// Total duration for parsing the page indexes (page index and column index)
    index_parsing_duration: Duration,
}

impl Display for Timing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "num_runs: {}", self.num_runs)?;
        writeln!(
            f,
            "    Metadata parsing time {:?}",
            self.metadata_parsing_duration / self.num_runs as u32
        )?;
        writeln!(
            f,
            "    PageIndex (Column/Offset) {:?}",
            self.index_parsing_duration / self.num_runs as u32
        )?;
        Ok(())
    }
}

impl Timing {
    pub fn avg_metadata_parsing_duration(&self) -> Duration {
        self.metadata_parsing_duration  / self.num_runs as u32
    }
    pub fn avg_index_parsing_duration(&self) -> Duration {
        self.index_parsing_duration  / self.num_runs as u32
    }
}

#[derive(Debug)]
pub struct MetadataParseResult {
    description: String,
    /// Timing for arrow-rs 56 (using thrift compiler)
    arrow_56_timing: Timing,
    /// Timing for arrow-rs 57 (using custom thrift parser)
    arrow_57_timing: Timing,
    /// Timing for unmodified arrow-rs main (unreleased 60.0.0)
    arrow_60_timing: Timing,
    /// Timing for arrow-rs with `ColumnChunkMask` support, selecting a single column
    /// when decoding the PageIndex (see https://github.com/apache/arrow-rs/pull/11157)
    page_index_selection_timing: Timing,

}

impl MetadataParseResult {
    pub fn set_headers(table: &mut Table) {
        table.set_header(vec![
            "Description",
            "Parse Time Arrow 56\n\nMetadata",
            "Parse Time Arrow 56\n\nPageIndex (Column/Offset)",
            "Parse Time Arrow 57\n\nMetadata",
            "Parse Time Arrow 57\n\nPageIndex (Column/Offset)",
            "Parse Time Arrow 60\n\nMetadata",
            "Parse Time Arrow 60\n\nPageIndex (Column/Offset)",
            "Parse Time Page Index Selection\n\nMetadata",
            "Parse Time Page Index Selection\n\nPageIndex (single column)",
        ]);
    }

    pub fn csv_headers() -> Vec<&'static str> {
        vec![
            "Description",
            "Parse Time Arrow 56 Metadata (ns)",
            "Parse Time Arrow 56 PageIndex (Column/Offset) (ns)",
            "Parse Time Arrow 57 Metadata (ns)",
            "Parse Time Arrow 57 PageIndex (Column/Offset) (ns)",
            "Parse Time Arrow 60 Metadata (ns)",
            "Parse Time Arrow 60 PageIndex (Column/Offset) (ns)",
            "Parse Time Page Index Selection Metadata (ns)",
            "Parse Time Page Index Selection PageIndex (single column) (ns)",
        ]
    }

    pub fn to_csv_row(&self) -> Vec<String> {
        vec![
            self.description.clone(),
            self.arrow_56_timing.avg_metadata_parsing_duration().as_nanos().to_string(),
            self.arrow_56_timing.avg_index_parsing_duration().as_nanos().to_string(),
            self.arrow_57_timing.avg_metadata_parsing_duration().as_nanos().to_string(),
            self.arrow_57_timing.avg_index_parsing_duration().as_nanos().to_string(),
            self.arrow_60_timing.avg_metadata_parsing_duration().as_nanos().to_string(),
            self.arrow_60_timing.avg_index_parsing_duration().as_nanos().to_string(),
            self.page_index_selection_timing.avg_metadata_parsing_duration().as_nanos().to_string(),
            self.page_index_selection_timing.avg_index_parsing_duration().as_nanos().to_string(),
        ]
    }
}

impl MetadataParseResult {
    pub fn add_to_table(&self, table: &mut Table) {
        table.add_row(vec![
            self.description.clone(),
            format!("{:?}", self.arrow_56_timing.avg_metadata_parsing_duration()),
            format!("{:?}", self.arrow_56_timing.avg_index_parsing_duration()),
            format!("{:?}", self.arrow_57_timing.avg_metadata_parsing_duration()),
            format!("{:?}", self.arrow_57_timing.avg_index_parsing_duration()),
            format!("{:?}", self.arrow_60_timing.avg_metadata_parsing_duration()),
            format!("{:?}", self.arrow_60_timing.avg_index_parsing_duration()),
            format!("{:?}", self.page_index_selection_timing.avg_metadata_parsing_duration()),
            format!("{:?}", self.page_index_selection_timing.avg_index_parsing_duration()),
        ]);
    }
}

impl Display for MetadataParseResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MetadataParseResult:")?;
        writeln!(f, "  description: {}", self.description)?;
        writeln!(f, "  Arrow 56 Timing:")?;
        writeln!(f, "{}", self.arrow_56_timing)?;
        writeln!(f, "  Arrow 57 Timing:")?;
        writeln!(f, "{}", self.arrow_57_timing)?;
        writeln!(f, "  Arrow 60 Timing:")?;
        writeln!(f, "{}", self.arrow_60_timing)?;
        writeln!(f, "  Page Index Selection Timing (single column):")?;
        writeln!(f, "{}", self.page_index_selection_timing)?;
        Ok(())
    }
}


