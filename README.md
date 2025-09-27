# Apache Parquet Metadata Parsing Benchmarks

This repository contains benchmarks for Apache Parquet metadata parsing for the Rust implementation
in [arrow-rs](https://github.com/apache/arrow-rs)
different libraries and configurations. The benchmarks measure the time taken to
read and parse Parquet metadata from files of various sizes.

It is designed to provide information about how fast we can make Parquet metadata parsing in Rust using 
the existing and [new thrift decoder]  in arrow-rs and help inform decisions about future improvements
and optimizations.

Also in the context of evalating a proposal to add a new footer format to Apache Parquet 

https://lists.apache.org/thread/j9qv5vyg0r4jk6tbm6sqthltly4oztd3


[new thrift decoder]: https://github.com/apache/arrow-rs/issues/5854

# Results

(TODO chart here)


# Background

Apache Parquet is a popular columnar storage format for big data processing. It
is designed to be efficient for both storage and query performance. Parquet
files consist of a header, a series of row groups, and a footer. The footer
contains metadata about the file, including the schema, statistics, and other
information needed to read and process the data.

Footer parsing is a critical step in reading Parquet files, as it provides the
necessary information to interpret the data. The performance of footer parsing
can have a significant impact on the overall performance of reading Parquet
files, especially for files with many columns / row groups.

An often criticzed part of the Parquet format is that it uses [Apache Thrift]
for serialization of the metadata. Thrift is a flexible and efficient
serialization framework, but does not provide random access parsing. Other formats
such as [Flatbuffers] which do provide zero copy and random access parsing have been
proposed as alternatives given their theoretical performance advantages.

Despite the disadvantage of thrift, we have theorized in the [How Good is
Parquet for Wide Tables (Machine Learning Workloads) Really?] blog post that there
is still room for significant performance improvements in Parquet footer parsing
in Rust using the existing thrift format but improving the thrift decoder implementation
in arrow-rs.




[How Good is Parquet for Wide Tables (Machine Learning Workloads) Really?]: https://www.influxdata.com/blog/how-good-parquet-wide-tables/


# Running the Benchmarks

To run the benchmarks, [install Rust] . You can then
clone this repository and run the benchmarks using the following commands:

[install Rust]: https://www.rust-lang.org/tools/install

```bash
cargo run --release
```

# Bechmark description

The benchmarks measure the time taken to read and parse Parquet metadata from
files of various sizes.

They are designed to be "best case" aka show off how well the new thrift decoder does
in the best case (p99 / p999) as those are often the cases where the existing thrift
decoders are slowest.


The benchmarks use the following Parquet files:

| Name | DataType | Number of Columns | Row Groups |   |   |
|------|----------|-------------------|------------|---|---|
|      |          |                   |            |   |   |
|      |          |                   |            |   |   |
|      |          |                   |            |   |   |


* `float`: Floating point numbers 
* `string`: String columns (average length 80 characters)


Details about the written files
* XXX 

