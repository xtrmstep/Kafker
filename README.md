# CLI tool to manage snapshots of Kafka topics

Kafker is a CLI tool written on .NET Core. The current version supports only events in JSON format and can do the following operations:

- create a snapshot of a Kafka topic (text .DAT file)
- emit events from the snapshot to a Kafka topic (can preserve time intervals between messages)
- convert the snapshot files to CSV files using defined mapping (in current version we rely on [C#.Net Import/Export CSV Library](https://github.com/asmak9/CSVLibraryAK) for .NET Core)

## Commands

Use help command to get information about command arguments and options.

```bash
./kafker.exe -?
```

| Command | Options | Description |
|---------|---------|-------------|
|create||Create Kafka topic configuration template|
||config|It will be the name of configuration file and topic|
|extract||Read and save JSON events from Kafka topic to a snapshot file (.DAT). It reads text and save to file "AS IS" without transformations. The Kafka message timestamp is stored along the event|
||config|The name of topic configuration|
||brokers|Kafka brokers|
||topic|Kafka topic name|
||number|Maximum number of extarcted events|
||offset|Offset of Kafka topic (Earliest or Latest)|
|emit||Emitting events from snapshot file to Kafka topic in JSON format|
||config|The topic configuration name|
||brokers|Kafka brokers|
||topic|Kafka topic name|
||number|Maximum number of emitted events|
||preserve|When set emitting preserves time interval between events. Intervals are calculated from stored timestamps of original messages.|
|convert||Convert snapshot to CSV file|
||config|The name of topic configuration with field mapping|
|list||List existing topic configurations|

## Configuration

### Kafker Settings

The configuration is located in `appsettings.json` file under the section `KafkerSettings`. Kafker has it's own configuration where you need to specify the following settings:

|Setting|Description|
|-------|-----------|
|Destination|Folder with snapshot files |
|ConfigurationFolder|Folder with topic configuration files|
|Brokers|Kafka brokers|

Destination and configuration folders could be the same folder. Brokers in this configuration will be used as default brockers for any Kafker commands. 

### Topic Configuration

It's not required to have a topic configuration to use Kafker. The topic configurations are named and store settings for quick access. Example of CFG file stored in `simple-topic.cfg`:

```json
{
  "Brokers": [
    "localhost:9092"
  ],
  "Topic": "my-topic",
  "EventsToRead": 50,
  "OffsetKind": "Latest",
  "Mapping" : {
        "Property" : "destination_property_name",
        "Node.Property" : "destination_property_of_nested_type",
        "Node.Array[1]" : "destination_property_of_array_element"   
        }
}
```

A simple command which uses this topic configuration:

```bash
$ kafker.exe extract -cfg simple-topic
```

It will extract 50 events from topic `my-topic` using `Latest` offset. A snapshot file will be created at the folder specified in `Destination` settings of Kafker configuration.

#### Mapping

The mapping is used for conversion to CSV file. It determines the mapping from properties of JSON (in flatten format) to fields in a CSV file. If the mapping is empty, then all fields will be taken and field names will get flatten property names. If it's specified, it won't include other fields to CSV file.   

### Overrides 

Any setting from configuration could be overriden. When you use Kafker without topic configuration, it will connect to Kafka brokers specified in Kafker settings. But you can override brokers and specify other configuration parameters. For example, possible commands to extarct events with overrides:

```bash
# override brokers, topic name, offset and number of events to read
$ kafker.exe extract -b localhost:9092 -t test -o earliest -n 10

# it's possible to use less options
$ kafker.exe extract -b localhost:9092 -t test -o earliest

# without topic configuration Kafker need to know which topic to read
$ kafker.exe extract -t test

# with topic configuration you still can override topic name
$ kafker.exe extract -cfg simple-topic -t test

# and more...
$ kafker.exe extract -cfg simple-topic -t test -n 3 -b localhost:9092
```

This command won't be successfull becasue there is no information about Kafka topic and topic configuration is also not specified :

```bash
$ kafker.exe extract -b localhost:9092 -o earliest -n 10
```

## CSV Conversion

Snapshots could be converted to CSV files for analysis. Kafker will try to parse events to JSON and merge them to the same table, so that the table will aquire field from all events. It uses [Json.NET](https://www.newtonsoft.com/json), [JsonFlatten](https://github.com/GFoley83/JsonFlatten) and [CsvHelper](https://joshclose.github.io/CsvHelper/).

If the mapping is specified in the topic confgiuration, Kafker will narrow down the number of fields and use names from the mapping.

That's it.
