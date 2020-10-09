# CLI tool to extract a Kafka topic to a CSV file

Kafker is a CLI tool written on .NET Core. It is reading a Kafka topic and stores events into a CSV file. It works with topics which have messages in JSON format.

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

- `topic.cfg` - a file with information about Kafka broker endpoints and topic name, offset and other parameters (see below) 
- `topic.map` - a file with information about mapping for this topic

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
        "destination_property_name" : "Property",
        "destination_property_of_nested_type" : "Node.Property",
        "destination_property_of_array_element" : "Node.Array[1]"   
        }
    
}
```

### JSON to CSV mapping

The tool will try to parse Kafka event to JSON. It uses [Json.NET](https://www.newtonsoft.com/json), [JsonFlatten](https://github.com/GFoley83/JsonFlatten) and [CsvHelper](https://joshclose.github.io/CsvHelper/).

When message is deserialized to JSON, the tool will try to use provided map-file to extract fields. If the map-file is not provided, it flatten JSON using `.` as a separator for nested properties.

Example of MAP file:

```json
{
  "Mapping": {
    "file_field": "Json.Property[0].Name"
  }
}
```

## Usage Examples

Let's read some topic and send extracted events to another topic. Before using this steps you need to configure `appsetting.json`, specify `ConfigurationFolder` where all configurations will be stored and `Destination` where all CSV files (extracted topics) will stored.  

Create templates

```bash
./kafker.exe create source-topic
```

This command will create one file: `source-topic.cfg`. In CFG file you need to specify Kafka broker(s) and exact topic name. Also you may specify number of events to read (`EventsToRead`) and other parameters. In the map section you may want to specify a mapping. If there is no mapping, then all fields will be extracted. Let's extract all fields. 

```json
{
  "Mapping": {
  }
}
```

You can check which topic configurations you have:

```bash
./kafker.exe list
```

Now let's extract events from the topic:

```bash
./kafker.exe extract -t source-topic
```

This command will read those two CFG and MAP files, read certain number of events (press Ctrl+C to break the operation earlier if you need). When it's finished in the destination folder (defined in the `appsetting.json`) you'll find a CSV file. The name of the file will have topic's name and timestamp in its name (e.g., `source-topic_20200825_054112.csv`).

Now let's send this file to another topic. You need to create a new template and update it with new information. If you're not specifying the mapping, JSON field in th Kafka event will be called same as in the file. The following command will read lines from file and emit them to Kafka topic.

```bash
./kafker.exe emit -t destination-topic c://csv_files/source-topic_20200825_054112.csv
```

That's it.
