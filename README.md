# Apache Parquet [Metadata Parsing Benchmarks]

[Metadata Parsing Benchmarks]: https://github.com/alamb/parquet_footer_parsing

## Summary

This benchmarks demonstrates nearly an order of magnitude improvement (8x)
parsing Parquet metadata with **no changes to the Parquet format**, by simply
writing a more efficient thrift decoder.

While we have not implemented a similar decoder in other languages such as C/C++
or Java, given the similarities in the existing thrift libraries and usage, we
expect similar improvements are possible in those languages as well.

<img src="results.png" width="800"/>

**Figure 1**: Benchmark results for [Apache Parquet] metadata parsing using the [new thrift decoder] in [arrow-rs], scheduled for release in 
[57.0.0]. No changes are needed to the Parquet format itself.

<img src="scaling.png" width="800"/>

**Figure 2**: Speedup for Apache Parquet metadata parsing for varying data types and column counts.

[Apache Parquet]: https://parquet.apache.org/
[arrow-rs]: https://github.com/apache/arrow-rs
[57.0.0]: https://github.com/apache/arrow-rs/issues/7835


*Note 1: the "no stats" version is a modified version of the new thrift parser
that skips over all index structures entirely, including statistics on column
chunks as well as page and offset indexes.*

*Note 2: These results show the theoretical best case improvements (e.g. when
doing point lookups in Parquet files using an external index, as explained in
the [Using External Indexes, Metadata Stores, Catalogs and Caches to Accelerate
Queries on Apache Parquet]). Most workloads will see more modest improvements.*

[Using External Indexes, Metadata Stores, Catalogs and Caches to Accelerate Queries on Apache Parquet]: https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/
[Apache DataFusion]: https://datafusion.apache.org/

## Introduction

Recently, the Parquet community has been evaluating [a proposal to add a new
footer format to Apache Parquet] (also a direct link to the [doc]) to address
some of the perceived shortcomings of the existing thrift format, including
the lack of random access parsing and the complexity of the thrift format.

[a proposal to add a new footer format to Apache Parquet]: https://lists.apache.org/thread/j9qv5vyg0r4jk6tbm6sqthltly4oztd3
[doc]: https://docs.google.com/document/d/1kZS_DM_J8n6NKff3vDQPD1Y4xyDdRceYFANUE0bOfb0/edit?tab=t.0#heading=h.ccu4zzsy0tm5

In parallel, the arrow-rs community has been exploring ways to improve the
performance of Parquet metadata parsing in Rust using the existing thrift
format, by implementing a more efficient [new thrift decoder]. In addition to
avoiding some overheads, the new decoder also allows for skipping over unneeded
fields more easily, which can be a significant performance improvement for wide
tables with many columns and row groups.

Thus, the natural question arises of how much performance improvement is possible
with this new thrift decoder, and how does it compare to the proposed new footer
format. See the [benchmarking ticket] for more context and discussion.


[benchmarking ticket]: https://github.com/apache/arrow-rs/issues/8441
[new thrift decoder]: https://github.com/apache/arrow-rs/issues/5854


## Background

Apache Parquet is a popular columnar storage format for big data processing. It
is designed to be efficient for both storage and query performance. Parquet
files consist of a header, a series of row groups, and a footer. The footer
contains metadata about the file, including the schema, statistics, and other
information needed to read and process the data.

Footer parsing is a critical step in reading Parquet files, as it provides the
necessary information to interpret the data. For systems that do not cache the
parsed footer, the performance of footer parsing can have a significant impact
on  overall query performance files, especially for files with many columns /
row groups.

An often criticized part of the Parquet format is that it uses [Apache Thrift]
for serialization of the metadata. Thrift is a flexible and efficient
serialization framework, but does not provide random access parsing. Other
formats such as [Flatbuffers] which do provide zero copy and random access
parsing have been proposed as alternatives given their theoretical performance
advantages. However, changing the Parquet format is a significant undertaking,
and requires buy-in from the community and ecosystem and can take years to be
adopted.

Despite the very real disadvantage of thrift, we have previously theorized in 
[How Good is Parquet for Wide Tables (Machine Learning Workloads) Really?] that
there is still room for significant performance improvements in Parquet footer
parsing in Rust using the existing thrift format but improving the thrift
decoder implementation.

[How Good is Parquet for Wide Tables (Machine Learning Workloads) Really?]: https://www.influxdata.com/blog/how-good-parquet-wide-tables/

## Running the Benchmark

To run the benchmarks, first [install Rust], and then clone this repository and
run the benchmarks using the following commands:

```shell
cargo run --release
```

[install Rust]: https://www.rust-lang.org/tools/install

## Benchmark Description

### Datasets

The benchmark makes several parquet files with the following characteristics:

| Name               | Description                                                                     |
|--------------------|---------------------------------------------------------------------------------|
| columns            | The number of columns in the schema                                             |
| row groups         | Each file has 20 row groups                                                     |
| rows per row group | Each row group has 1000 rows                                                    |
| DataType: Float | The columns are Float32                                                         |
| DataType: String| The columns are String (avg length 10 characters, max length 20 characters)     |

You can examine details of the metadata parquet files in the `data` directory using `datafusion-cli`:

```shell
datafusion-cli -c "select * from parquet_metadata('output/String_data_100_cols.parquet');
```

### Decoders / Configurations`

| Name                   | Description                                                                                                                                                                       |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Arrow 56`             | Using the [released version of parquet 56.2.0]                                                                                                                                    |
| `Arrow 57`             | Using a [snapshot](https://github.com/alamb/arrow-rs/tree/alamb/thrift-remodel-snapshot) of the remodel branch (based on [this PR](https://github.com/apache/arrow-rs/pull/8476)) |
| `Arrow 57 (no stats)`  | A modification to the above, manually updated to skip parsing all index structures (see [changes in this PR](https://github.com/alamb/arrow-rs/pull/54))                          |

[released version of parquet 56.2.0]:https://crates.io/crates/parquet/56.2.0

`Arrow 57 (no stats)` shows the theoretical best case once arrow-rs offers and
API to selectively skip parsing of unnecessary fields [see this
ticket](https://github.com/apache/arrow-rs/issues/5855), such as statistics for
columns which do not have predicates on them. The version in this benchmark
skips both [statistics on column chunks] as well as the [PageIndex].

[statistics on column chunks]: https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/src/main/thrift/parquet.thrift#L912-L939
[PageIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md

# Results

As shown in Figure 1 and 2, across the board, we see a 8x speedup (86% reduction) for
decoding metadata when using the new thrift decoder in arrow-rs and skipping the
parsing of statistics and index structures entirely. Without skipping the
parsing of statistics and index structures, we see about a 3.3x speedup overall 


For example, with the
`String 100000 cols 20 row groups ` dataset, we go from a total time of 3.63s
`(1.19s + 2.13s = 3.32s)` to 0.418s `(0.418s + 0s = 0.418s)`


This is roughly in line with the 80% performance reduction results
@adrian-thurston saw in internal benchmarks of InfluxData production workloads,
when being more deliberate about which PageIndexes were decoded. See [Reduce
page metadata loading to only what is necessary for query execution in
ParquetOpen #16200] for more details. 

[Reduce page metadata loading to only what is necessary for query execution in ParquetOpen #16200]: https://github.com/apache/datafusion/issues/16200

Results (see also [Spreadsheet] for source and diagrams)

[Spreadsheet]: https://docs.google.com/spreadsheets/d/1Ypsox5EywNmv9ORwrlmJlWcPVvWlOW_QCnIt_U68vbo/edit?gid=1818026620#gid=1818026620

## Results on Mac OS
System configuration: Apple M3 Max, 16-inch, Nov 2023 64GB RAM

```text
+-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------+
| Description                       | Parse Time Arrow 56 | Parse Time Arrow 56       | Parse Time Arrow 57 | Parse Time Arrow 57       | Parse Time Arrow 57 (no stats) | Parse Time Arrow 57 (no stats) |
|                                   |                     |                           |                     |                           |                                |                                |
|                                   | Metadata            | PageIndex (Column/Offset) | Metadata            | PageIndex (Column/Offset) | Metadata                       | PageIndex (Column/Offset)      |
+=========================================================================================================================================================================================================+
|  Float 100 cols 20 row groups     | 1.294875ms          | 2.073475ms                | 378.395µs           | 302.866µs                 | 369.862µs                      | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  Float 1000 cols 20 row groups    | 11.63472ms          | 19.0351ms                 | 3.749433ms          | 3.003558ms                | 3.64202ms                      | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  Float 10000 cols 20 row groups   | 129.81765ms         | 207.331179ms              | 41.349737ms         | 39.355841ms               | 37.785258ms                    | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  Float 100000 cols 20 row groups  | 1.315675716s        | 2.083634491s              | 415.187495ms        | 449.226879ms              | 382.43787ms                    | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 100 cols 20 row groups    | 1.068729ms          | 1.925558ms                | 423.562µs           | 382.321µs                 | 353.462µs                      | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 1000 cols 20 row groups   | 10.482854ms         | 18.943345ms               | 4.247812ms          | 3.774325ms                | 3.479541ms                     | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 10000 cols 20 row groups  | 120.658891ms        | 209.021562ms              | 45.431041ms         | 47.823499ms               | 36.33685ms                     | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 100000 cols 20 row groups | 1.227258745s        | 2.152299291s              | 463.356495ms        | 552.433658ms              | 367.39845ms                    | 0ns                            |
+-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------+
```

CSV output:
```csv
Description,Parse Time Arrow 56 Metadata (ns),Parse Time Arrow 56 PageIndex (Column/Offset) (ns),Parse Time Arrow 57 Metadata (ns),Parse Time Arrow 57 PageIndex (Column/Offset) (ns),Parse Time Arrow 57 (no stats) Metadata (ns),Parse Time Arrow 57 (no stats) PageIndex (Column/Offset) (ns)
Float 100 cols 20 row groups,1294875,2073475,378395,302866,369862,0
Float 1000 cols 20 row groups,11634720,19035100,3749433,3003558,3642020,0
Float 10000 cols 20 row groups,129817650,207331179,41349737,39355841,37785258,0
Float 100000 cols 20 row groups,1315675716,2083634491,415187495,449226879,382437870,0
String 100 cols 20 row groups,1068729,1925558,423562,382321,353462,0
String 1000 cols 20 row groups,10482854,18943345,4247812,3774325,3479541,0
String 10000 cols 20 row groups,120658891,209021562,45431041,47823499,36336850,0
String 100000 cols 20 row groups,1227258745,2152299291,463356495,552433658,367398450,0
```

## Results on Linux

System configuration:
AWS EC2: `m6id.8xlarge`
32 core (Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz)
128GB RAM
Ubuntu 24.04.3 LTS

```text
+-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------+
| Description                       | Parse Time Arrow 56 | Parse Time Arrow 56       | Parse Time Arrow 57 | Parse Time Arrow 57       | Parse Time Arrow 57 (no stats) | Parse Time Arrow 57 (no stats) |
|                                   |                     |                           |                     |                           |                                |                                |
|                                   | Metadata            | PageIndex (Column/Offset) | Metadata            | PageIndex (Column/Offset) | Metadata                       | PageIndex (Column/Offset)      |
+=========================================================================================================================================================================================================+
|  Float 100 cols 20 row groups     | 2.822881ms          | 4.361313ms                | 846.353µs           | 666.451µs                 | 499.013µs                      | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  Float 1000 cols 20 row groups    | 27.067827ms         | 43.140346ms               | 6.967499ms          | 6.347585ms                | 5.021332ms                     | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  Float 10000 cols 20 row groups   | 289.293586ms        | 508.958878ms              | 75.802371ms         | 76.216344ms               | 54.793877ms                    | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  Float 100000 cols 20 row groups  | 2.951604202s        | 5.231709739s              | 755.788675ms        | 758.020451ms              | 550.973199ms                   | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 100 cols 20 row groups    | 2.559624ms          | 4.055637ms                | 1.028411ms          | 827.057µs                 | 485.581µs                      | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 1000 cols 20 row groups   | 23.93038ms          | 39.94606ms                | 8.137762ms          | 7.915083ms                | 4.909874ms                     | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 10000 cols 20 row groups  | 258.078736ms        | 452.136477ms              | 87.183576ms         | 90.996418ms               | 53.630656ms                    | 0ns                            |
|-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------|
|  String 100000 cols 20 row groups | 2.649973875s        | 4.702651945s              | 872.627971ms        | 926.296932ms              | 540.400121ms                   | 0ns                            |
+-----------------------------------+---------------------+---------------------------+---------------------+---------------------------+--------------------------------+--------------------------------+
```

CSV output:
```csv
Description,Parse Time Arrow 56 Metadata (ns),Parse Time Arrow 56 PageIndex (Column/Offset) (ns),Parse Time Arrow 57 Metadata (ns),Parse Time Arrow 57 PageIndex (Column/Offset) (ns),Parse Time Arrow 57 (no stats) Metadata (ns),Parse Time Arrow 57 (no stats) PageIndex (Column/Offset) (ns)
Float 100 cols 20 row groups,2822881,4361313,846353,666451,499013,0
Float 1000 cols 20 row groups,27067827,43140346,6967499,6347585,5021332,0
Float 10000 cols 20 row groups,289293586,508958878,75802371,76216344,54793877,0
Float 100000 cols 20 row groups,2951604202,5231709739,755788675,758020451,550973199,0
String 100 cols 20 row groups,2559624,4055637,1028411,827057,485581,0
String 1000 cols 20 row groups,23930380,39946060,8137762,7915083,4909874,0
String 10000 cols 20 row groups,258078736,452136477,87183576,90996418,53630656,0
String 100000 cols 20 row groups,2649973875,4702651945,872627971,926296932,540400121,0
```


