# CLI tool to manage snapshots of Kafka topics

Kafker is a CLI tool written on .NET Core. The current version supports only events in JSON format and can do the following operations:

- create a snapshot of a Kafka topic (text .DAT file)
- emit events from the snapshot to a Kafka topic (in current version regardless to stored message timestamp)
- convert the snapshot files to CSV files using defined mapping (in current version we rely on [C#.Net Import/Export CSV Library](https://github.com/asmak9/CSVLibraryAK) for .NET Core)

## Configurations

### Reading a Topic

There are several options to read Kafka topic:

- from beginning (earliest)
- only new events (latest)

In all cases you can specify a number of events to be read. By default the tool reads topic until it's stopped with Ctrl+C.

The simplest command to extract a topic to CSV file is as follows:

```bash
./kafker.exe --topic topic
```

This command relies on existing configuration with name `topic`. It will produce a file `topic-<date>-<time>.csv` with serialized events in table form. The configuration should be located in working folder or current folder and consists of following artifacts:

- `topic.cfg` - a file with information about Kafka broker endpoints and topic name, offset and other parameters (see below) additionally information about mapping for this topic.

#### Parameters

The following command will show all possible commands and options for this CLI.

```bash
./kafker.exe --help
```

Any of this arguments can be used in CFG file to set a default value. If CFG file has a value for a command or option it will be overriden with explicitly specified one in command line.

Example of CFG file:

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

### JSON to CSV mapping

The tool will try to parse Kafka event to JSON. It uses [Json.NET](https://www.newtonsoft.com/json), [JsonFlatten](https://github.com/GFoley83/JsonFlatten) and [CsvHelper](https://joshclose.github.io/CsvHelper/).

When message is deserialized to JSON, the tool will try to use provided mapping to keep only specified fields in the result CSV file. If the mapping is not provided, all fields will be included.

Example of the mapping, which describes mapping of nested property to a column:

```json
{
  "Mapping": {
    "Json.Property[0].Name": "column_name_in_csv"
  }
}
```

## Usage Examples

Let's read some topic and send extracted events to another topic. Before using this steps you need to configure `appsetting.json`, specify `ConfigurationFolder` where all configurations will be stored and `Destination` where all snapshot files (extracted topics) will stored.  

### Create templates

```bash
./kafker.exe create source-topic
```

This command will create one file: `source-topic.cfg`. In CFG file you need to specify Kafka broker(s) and topic name. Also you may specify number of events to read (`EventsToRead`) and other parameters. In the section `Mapping` you may want to specify a mapping. Let's extract all fields, so that we specify an empty mapping. 

```json
{
    Brokers : ["localhost:9092"],
    Topic : "topic_name",
    EventsToRead : 10,
    OffsetKind : "Earliest",
    Mapping : {
    }
}
```

### List all configurations

You can check which topic configurations you have:

```bash
./kafker.exe list
```

### Snapshot a topic 

Now let's extract events from the topic:

1. ###### Extract event with config file.

You can extract events from topic either by specifying a config file as shown below:

```bash
./kafker.exe extract -cfg:source-config
```

Additionally you are allowed to override the configuration of the source-config as shown below:

```
./kafker.exe extract -cfg:source-config -n:10
```

Note: You can either override all or as many as you want arguments. 

2. ###### Extract event without additional config file.

You can directly specify the configuration

```bash
./kafker.exe extract -b:source-brokers -t:source-topic -n:number-of-events -o:offset
```

Note: When you specify the configuration straight from the console keep in mind that the topic source is mandatory and should be specified, the rest of the arguments are optional. If omitted their default value will be used.

This command will read the config file, read certain number of events (press Ctrl+C to break the operation earlier if you need). When it's finished in the destination folder (defined in the `appsetting.json`) you'll find a DAT file. The name of the file will have configuration's name and timestamp in its name (e.g., `source-topic_20200825_054112.dat`).

### Convert snapshot to CSV

You need to specify the configuration and .DAT file from which you want to create a CSV file. The configuration is required to specify the mapping.

```bash
./kafker.exe convert -t source-topic source-topic_20200825_054112.dat
```

You can omit the argument `-t` to convert the snapshot as if there is no mapping specified. 

### Emit snapshot to a Kafka topic

The emit command will emit events to the topic specified in the configuration file. If you need to emit to another topic, you may want to create another configuration file.

```bash
./kafker.exe emit -t destination-topic source-topic_20200825_054112.dat
```

## Notes

* Kafker lookup for files using the following order:
    * in the current folder
    * in the folder specified in the tool's configuration
    * using absolute path
* Produced CSV files are stored near source .DAT files  

That's it.

