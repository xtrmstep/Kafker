# CLI tool to extract Kafka topic to CSV file

The tool is reading a Kafka topic and stores events into CSV file. The topic should be in JSON format.

## Configurations

### Reading a Topic

There are several options to read Kafka topic:

- from beginning (earliest)
- only new events (latest)
- from specific offset

In all cases you can specify a number of events to be read. By default the tool reads topic until it's topped with Ctrl+C.

The simplest command to extract a topic to CSV file is as follows:

```bash
./kafka-topic-extractor.exe --topic topic
```
This command will produce a file `topic-<date>-<time>.csv` with serialized events. For this command to work you ned to have in the working folder (or current folder) following artifacts:

- `topic.cfg` - a file with information about Kafka broker endpoints and topic name, offset and other parameters (see below) 
- `topic.map` - a file with information about mapping for this topic

#### Parameters

The following command will show all possible commands and options for this CLI.

```bash
./kafka-topic-extractor.exe --help
```

Any of this arguments can be used in CFG file to set a default value. If CFG file has a value for a command or option it will be overriden with explicitly specified one in command line.

Example of CFG file:

```txt
to be updated
```  

### JSON to CSV mapping

The tool will try to parse Kafka event to JSON. It uses [Json.NET](https://www.newtonsoft.com/json), [JsonFlatten](https://github.com/GFoley83/JsonFlatten) and [CsvHelper](https://joshclose.github.io/CsvHelper/).

When message is deserialized to JSON, the tool will try to use provided map-file to extract fields. If the map-file is not provided, it flatten JSON using `.` as a separator for nested properties.

Example of MAP file:

```txt
to be updated
``` 