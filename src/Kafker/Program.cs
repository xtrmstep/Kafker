﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Kafker.Commands;
using Kafker.Configurations;
using Kafker.Helpers;
using Kafker.Kafka;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

[assembly: InternalsVisibleTo("Kafker.Tests")]

namespace Kafker
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");
            var configuration = CreateConfiguration(environment);

            var kafkerSettings = configuration.GetSection(nameof(KafkerSettings)).Get<KafkerSettings>();
            var configSettings = configuration.GetSection(nameof(KafkaTopicConfiguration)).Get<KafkaTopicConfiguration>();

            var services = CreateServiceProvider(kafkerSettings, configSettings);

            using var app = new CommandLineApplication
            {
                Name = "kafker",
                Description = "CLI to extract Kafka topic with JSON events to CSV file"
            };
            app.Conventions
                .UseDefaultConventions()
                .UseConstructorInjection(services);

            app.Command("extract", p =>
            {
                p.Description = "Extract a topic events to a snapshot (.DAT) file";

                var configArg = p.Option("-cfg|--config <CONFIG>", "Configuration file",
                    CommandOptionType.SingleOrNoValue);
                var brokers = p.Option("-b|--broker <BROKER>", "Broker", CommandOptionType.MultipleValue);
                var topicName = p.Option("-t|--topic <TOPIC>", "Topic name from where the snapshot will be extracted",
                    CommandOptionType.SingleOrNoValue);
                var eventsToRead = p.Option("-n|--number <NUMBER>", "Number of events to read",
                    CommandOptionType.SingleOrNoValue);
                var offSetKind = p.Option("-o|--offset <OFFSET>", "Option to read Kafka topic from earliest or only new events",
                    CommandOptionType.SingleOrNoValue);
                p.OnExecuteAsync(async cancellationToken =>
                {
                    var argumentList = new Dictionary<string, string>();
                    var shouldOverrideConfigFile = false;

                    try
                    {
                        if (!configArg.HasValue() && !topicName.HasValue())
                        {
                            await PhysicalConsole.Singleton.Out.WriteLineAsync($"Topic should be specified");
                            return 1;
                        }
                        foreach (var item in p.Options.Where(item => item.HasValue()))
                        {
                            argumentList.Add(item.LongName, item.Value());
                        }

                        if (configArg.HasValue() && argumentList.Count > 1)
                        {
                            shouldOverrideConfigFile = true;
                        }

                        var extractCommand = services.GetService<IExtractCommand>();
                        return await extractCommand.InvokeAsync(cancellationToken, configArg.Value(), argumentList, shouldOverrideConfigFile);
                    }
                    catch (Exception err)
                    {
                        await PhysicalConsole.Singleton.Out.WriteLineAsync($"An error has occurred: {err.Message}");
                        app.ShowHelp();
                        
                    }

                    return 0;
                });
            });

            app.Command("create", p =>
            {
                p.Description = "Create a template topic configuration file";

                var nameArg = p.Option("-cfg|--config <CONFIG>", "Configuration file",
                    CommandOptionType.SingleValue);

                p.OnExecuteAsync(async cancellationToken =>
                {
                    var count = p.Options.Count;
                    var createTemplateCommand = services.GetService<ICreateCommand>();
                    return await createTemplateCommand.InvokeAsync(cancellationToken, nameArg.Value());
                });
            });

            app.Command("list", p =>
            {
                p.Description = "List existing configurations";
                p.OnExecuteAsync(async cancellationToken =>
                {
                    var listCommand = services.GetService<IListCommand>();
                    return await listCommand.InvokeAsync();
                });
            });

            app.Command("emit", p =>
            {
                p.Description = "Emit events from a given snapshot file (.DAT)";

                var topicArg = p.Option("-t|--topic <TOPIC>", "Topic name to which events should be emitted",
                    CommandOptionType.SingleValue).IsRequired();
                var preserveArg = p.Option("-p|--preserve <PRESERVE>", "", CommandOptionType.SingleOrNoValue);
                var fileName = p.Argument("file", "Relative or absolute path to a DAT file with topic snapshot")
                    .IsRequired();

                p.OnExecuteAsync(async cancellationToken =>
                {
                    if (preserveArg.HasValue())
                    {
                        services = CreateServiceProvider(kafkerSettings, configSettings, collection => collection.AddSingleton<IEventsEmitter, EventsEmitterPreserveTime>());
                    }
                    else
                    {
                        services = CreateServiceProvider(kafkerSettings, configSettings,
                            collection => collection.AddSingleton<IEventsEmitter, SimpleEventsEventsEmitter>());
                    }

                    var emitCommand = services.GetService<IEmitCommand>();
                    return await emitCommand.InvokeAsync(cancellationToken, topicArg.Value(), fileName.Value);
                });
            });

            app.Command("convert", p =>
            {
                p.Description = "Convert JSON snapshot to a CSV file";

                var fileName = p.Argument("file", "Relative or absolute path to a DAT file with topic snapshot")
                    .IsRequired();

                var topicArg = p.Option("-cfg|--config <CONFIG>", "Configuration file",
                    CommandOptionType.SingleOrNoValue);

                p.OnExecuteAsync(async cancellationToken =>
                {
                    var convertCommand = services.GetService<IConvertCommand>();
                    return await convertCommand.InvokeAsync(cancellationToken, fileName.Value, topicArg.Value());
                });
            });

            app.OnExecuteAsync(async cancellationToken =>
            {
                await PhysicalConsole.Singleton.Error.WriteLineAsync("Specify a command");
                app.ShowHelp();
                return await Task.FromResult(0).ConfigureAwait(false);
            });

            try
            {
                return await app.ExecuteAsync(args).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await PhysicalConsole.Singleton.Out.WriteLineAsync($"An error has occurred: {e.Message}");
                app.ShowHelp();
            }

            return 0;
        }

        private static ServiceProvider CreateServiceProvider(KafkerSettings kafkerSettings, KafkaTopicConfiguration configSettings, Action<IServiceCollection> addAdditionalServices = null)
        {
            var servicesCollection = new ServiceCollection()
                .AddSingleton<IConsumerFactory, ConsumerFactory>()
                .AddSingleton<IProducerFactory, ProducerFactory>()
                .AddSingleton<IFileTagProvider, FileTagProvider>()
                .AddSingleton<IExtractCommand, ExtractCommand>()
                .AddSingleton<ICreateCommand, CreateCommand>()
                .AddSingleton<IConvertCommand, ConvertCommand>()
                .AddSingleton<IListCommand, ListCommand>()
                .AddSingleton<IEmitCommand, EmitCommand>()
                .AddSingleton(PhysicalConsole.Singleton)
                .AddSingleton(kafkerSettings)
                .AddSingleton(configSettings);

            addAdditionalServices?.Invoke(servicesCollection);

            var services = servicesCollection.BuildServiceProvider();

            return services;
        }


        public static IConfigurationRoot CreateConfiguration(string environment)
        {
            environment ??= "Development";
            var env = environment.ToLowerInvariant();
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true, true)
                .AddJsonFile($"appsettings.{env}.json", true, true);
            var configuration = builder.Build();
            return configuration;
        }
    }
}