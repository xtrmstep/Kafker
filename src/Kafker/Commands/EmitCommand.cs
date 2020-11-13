using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
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

            //var shouldPreserve = preserve != false;
            var shouldPreserve = false;
            var timerList = new List<Timer>();
            var producedEvents = 0;
            using var topicProducer = _producerFactory.Create(cfg);
            if (!shouldPreserve)
            {
                try
                {
                    var getDictionary = await CreatePreserveTime(fileName);
                    using var stateObject = new PreserveTime();
                    var waitHandles = new List<WaitHandle>();
                    foreach (var item in getDictionary)
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        stateObject.ItemsToSend = item.Value;
                        stateObject.ProducerConfig = cfg;
                        stateObject.ResetEvent = new AutoResetEvent(false);
                        waitHandles.Add(stateObject.ResetEvent);
                        var delay = item.Key;
                        timerList.Add(new Timer(EmitEventsOnTime, stateObject, delay, Timeout.Infinite));
                        producedEvents++;
                    }

                    WaitHandle.WaitAll(waitHandles.ToArray());

                    int a = 0;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
                finally
                {
                    foreach (var item in timerList)
                    {
                        await item.DisposeAsync();
                    }

                    await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
                }
            }
            else
            {
                using var reader = new StreamReader(fileName);
                try
                {
                    string line;
                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        var pair = line.Split("|");
                        var jsonText = pair[1].Substring(1, pair[1].Length - 2);
                        await topicProducer.ProduceAsync(jsonText);
                        producedEvents++;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
                finally
                {
                    await _console.Out.WriteLineAsync($"\r\nProduced {producedEvents} events");
                }
            }

            return await Task.FromResult(0).ConfigureAwait(false); // ok
        }

        private void EmitEventsOnTime(object state)
        {
            if (state != null)
            {
                var preserver = (PreserveTime) state;
                
                using var topicProducer = _producerFactory.Create(preserver.ProducerConfig);
                foreach (var item in preserver.ItemsToSend)
                {
                    topicProducer.ProduceAsync(item).GetAwaiter().GetResult();
                }

                preserver.ResetEvent.Set();
            }
        }

        private async Task<Dictionary<long, List<string>>> CreatePreserveTime(string fileName)
        {
            var allLines = await File.ReadAllLinesAsync(fileName);
            var timeStampWithEvents = new List<Tuple<long, string>>();
            var eventsToEmit = new Dictionary<long, List<string>>();
            var initialSnapshot = allLines.Select(item => item.Split("|")).Select(pair => new Tuple<long, string>(long.Parse(pair[0].Replace("\"", "")), pair[1])).ToList();
            var smallest = initialSnapshot.Select(x => x.Item1).Min();

            foreach (var (timeStamp, itemToSend) in initialSnapshot)
            {
                timeStampWithEvents.Add(new Tuple<long, string>(timeStamp - smallest, itemToSend));
            }

            foreach (var (timeStamp, itemToSend) in timeStampWithEvents)
            {
                if (!eventsToEmit.ContainsKey(timeStamp))
                {
                    eventsToEmit.Add(timeStamp, new List<string>());
                }

                eventsToEmit[timeStamp].Add(itemToSend);
            }

            return eventsToEmit;
        }

        private class PreserveTime : IDisposable
        {
            public List<string> ItemsToSend { get; set; }
            public AutoResetEvent ResetEvent { get; set; }
            public KafkaTopicConfiguration ProducerConfig { get; set; }

            public void Dispose()
            {
                ResetEvent?.Dispose();
            }
        }
    }
}