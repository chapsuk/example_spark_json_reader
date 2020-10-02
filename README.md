# Spark RDD example

Spark on scala example.
Read json file, decode to User case class, print to output
 
## Build 

```shell
$ sbt assembly
```

## Run

```shell
$ spark-submit \
    --master local[*] \
    --class com.example.JsonReader \
    <assembly.jar> \
    <target.json>
```
