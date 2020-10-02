# Spark Examples



## Build

```shell
$ sbt assembly
```

## JsonReader 

Spark on scala example.
Read json file, decode to User case class, print to output

### Dataset

[:bookmark_tabs:](https://storage.googleapis.com/otus_sample_data/winemag-data.json.tgz)

### Run

```shell
$ spark-submit                      \
    --master local[*]               \
    --class com.example.JsonReader  \
    <assembly.jar>                  \
    <target.json>
```

## CrimeStrict

Aggregate data from boston crimes dataset and write to parquet file

## Dataset

[:bookmark_tabs:](https://www.kaggle.com/AnalyzeBoston/crimes-in-boston)

### Run

```shell
$ spark-submit                      \
    --master local[*]               \
    --class com.example.CrimeStrict \
    <assembly.jar>                  \
    <crimes.csv>                    \ 
    <offense.csv>                   \
    <output_dir>
```