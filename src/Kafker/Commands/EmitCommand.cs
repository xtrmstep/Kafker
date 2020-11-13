using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public class EmitCommand : IEmitCommand
    {
        private readonly IConsole _console;
        private readonly KafkerSettings _settings;
        private readonly IProducerFactory _producerFactory;

        public EmitCommand(IConsole console, KafkerSettings settings, IProducerFactory producerFactory)
        {
            _console = console;
            _settings = settings;
            _producerFactory = producerFactory;
        }

        public async Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName)
        {
            var cfg = await ExtractorHelper.ReadConfigurationAsync(topic, _settings, _console);

            try
            {
                var allLines = await File.ReadAllLinesAsync(fileName, cancellationToken);
                var a = new List<Tuple<long, string>>();
                var b = new List<Tuple<long, string>>();
                var c = new Dictionary<long, List<string>>();
                foreach (var item in allLines)
                {
                    var pair = item.Split("|");
                    a.Add(new Tuple<long, string>(long.Parse(pair[0].Replace("\"", "")), pair[1]));
                }

                long smallest = a[0].Item1;
                for (int i = 1; i < a.Count; i++)
                {
                    if (smallest > a[i].Item1)
                    {
                        smallest = a[i].Item1;
                    }
                }

                for (int i = 0; i < a.Count; i++)
                {
                    b.Add(new Tuple<long, string>(a[i].Item1 - smallest, a[i].Item2));
                }


                foreach (var item in b)
                {
                    if (!c.ContainsKey(item.Item1))
                    {
                        c.Add(item.Item1, new List<string>());
                    }
                    c[item.Item1].Add(item.Item2);
                }

                var list = new List<Timer>();
                foreach (var item in c)
                {
                    var jsons = item.Value;
                    var times = item.Key;
                    list.Add(new Timer(SendJsons,jsons,times,Timeout.Infinite));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            var producedEvents = 0;
            using var topicProducer = _producerFactory.Create(cfg);
            using (var reader = new StreamReader(fileName))
            {
                try
                {
                    string line;
                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var pair = line.Split("|");
                        //var timestamp = pair[0].Substring(1, pair[0].Length - 2);
                        var jsonText = pair[1].Substring(1, pair[1].Length - 2);
                        await topicProducer.ProduceAsync(jsonText);
                        producedEvents++;
                    }
                }
                finally
                {
                    await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
                }
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }

        private void SendJsons(object state)
        {
            if (state != null)
            {
                var jsonList = (List<string>)state;
                Parallel.ForEach(jsonList, s =>
                {
                    //produce
                    // topicProducer.ProduceAsync(jsonText).GetAwaiter().GetResult();
                    
                });
                
            }
        }
    }
}