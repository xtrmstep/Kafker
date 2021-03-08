using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Commands;
using Kafker.Configurations;
using Kafker.Emitters;
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

            var services = CreateServiceProvider(kafkerSettings);

            using var app = new CommandLineApplication
            {
                Name = "kafker",
                Description = "CLI tool to manage snapshots of Kafka topics"
            };
            app.Conventions
                .UseDefaultConventions()
                .UseConstructorInjection(services);

            app.Command("extract", p =>
            {
                p.Description = "Extract topic events to a snapshot (.DAT) file";

                var config = CommandOptionsFactory.ConfigOption(p);
                var brokers = CommandOptionsFactory.BrokersOption(p);
                var topic = CommandOptionsFactory.TopicOption(p);
                var eventsToExtract = CommandOptionsFactory.NumberOption(p);
                var offset = CommandOptionsFactory.OffsetOption(p);

                p.OnExecuteAsync(async cancellationToken =>
                {
                    try
                    {
                        var conf = await InitKafkaTopicConfigurationAsync(kafkerSettings, config, brokers, topic, eventsToExtract, offset);
                        
                        // validation
                        if (!conf.Brokers.Any()) throw new ArgumentException("Brokers are required", nameof(KafkaTopicConfiguration.Brokers));
                        if (string.IsNullOrWhiteSpace(conf.Topic)) throw new ArgumentException("Topic is required", nameof(KafkaTopicConfiguration.Topic));
                        
                        var extractCommand = services.GetService<IExtractCommand>();
                        return await extractCommand.InvokeAsync(cancellationToken, conf).ConfigureAwait(false);
                    }
                    catch (Exception err)
                    {
                        await PhysicalConsole.Singleton.Out.WriteLineAsync($"An error has occurred: {err.Message}");
                        app.ShowHelp();
                        return Constants.RESULT_CODE_ERROR;
                    }
                });
            });

            app.Command("create", p =>
            {
                p.Description = "Create topic configuration file";

                var config = CommandOptionsFactory.ConfigArgument(p);

                p.OnExecuteAsync(async cancellationToken =>
                {
                    var createTemplateCommand = services.GetService<ICreateCommand>();
                    return await createTemplateCommand.InvokeAsync(cancellationToken, config.Value);
                });
            });

            app.Command("list", p =>
            {
                p.Description = "List existing topic configurations";
                p.OnExecuteAsync(async cancellationToken =>
                {
                    var listCommand = services.GetService<IListCommand>();
                    return await listCommand.InvokeAsync();
                });
            });

            app.Command("emit", p =>
            {
                p.Description = "Emit events from a snapshot file (.DAT)";

                var config = CommandOptionsFactory.ConfigOption(p);
                var brokers = CommandOptionsFactory.BrokersOption(p);
                var topic = CommandOptionsFactory.TopicOption(p);
                var eventsToEmit = CommandOptionsFactory.NumberOption(p);
                var preserve = CommandOptionsFactory.PreserveOption(p);
                var fileName = CommandOptionsFactory.FileArgument(p);

                p.OnExecuteAsync(async cancellationToken =>
                {
                    // define services for emitter
                    var addEventsEmitterService = preserve.HasValue()
                        ? (Action<IServiceCollection>) (collection => collection.AddSingleton<IEventsEmitter, TimelyEventsEmitter>())
                        : collection => collection.AddSingleton<IEventsEmitter, SimpleEventsEmitter>();
                    services = CreateServiceProvider(kafkerSettings, addEventsEmitterService);
                    var emitCommand = services.GetService<IEmitCommand>();

                    var conf = await InitKafkaTopicConfigurationAsync(kafkerSettings, config, brokers, topic, eventsToEmit);
                    
                    // validation
                    if (!conf.Brokers.Any()) throw new ArgumentException("Brokers are required", nameof(KafkaTopicConfiguration.Brokers));
                    if (string.IsNullOrWhiteSpace(conf.Topic)) throw new ArgumentException("Topic is required", nameof(KafkaTopicConfiguration.Topic));
                    
                    return await emitCommand.InvokeAsync(cancellationToken, conf, fileName.Value);
                });
            });

            app.Command("convert", p =>
            {
                p.Description = "Convert snapshot to a CSV file";

                var config = CommandOptionsFactory.ConfigOption(p);
                var fileName = CommandOptionsFactory.FileArgument(p);

                p.OnExecuteAsync(async cancellationToken =>
                {
                    var topicConfiguration = await InitKafkaTopicConfigurationAsync(kafkerSettings, config);
                    var convertCommand = services.GetService<IConvertCommand>();
                    return await convertCommand.InvokeAsync(cancellationToken, fileName.Value, topicConfiguration);
                });
            });
            
            app.Command("cfg", p =>
            {
                p.Description = "Show default configuration";
                p.OnExecuteAsync(async cancellationToken =>
                {
                    var configCommand = services.GetService<IConfigCommand>();
                    return await configCommand.InvokeAsync();
                });
            });

            app.OnExecuteAsync(async cancellationToken =>
            {
                await PhysicalConsole.Singleton.Error.WriteLineAsync("Specify a command");
                app.ShowHelp();
                return await Task.FromResult(Constants.RESULT_CODE_OK).ConfigureAwait(false);
            });

            try
            {
                var source = new CancellationTokenSource();
                PhysicalConsole.Singleton.CancelKeyPress += (sender, args) => source.Cancel();
                return await app.ExecuteAsync(args, source.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await PhysicalConsole.Singleton.Out.WriteLineAsync($"An error has occurred: {e.Message}");
                app.ShowHelp();
            }

            return Constants.RESULT_CODE_OK;
        }

        private static async Task<KafkaTopicConfiguration> InitKafkaTopicConfigurationAsync(KafkerSettings kafkerSettings, CommandOption configName,
            CommandOption brokers = null, CommandOption topicName = null, CommandOption<uint> eventsToExtract = null, CommandOption<OffsetKind> offSetKind = null)
        {
            var events = eventsToExtract != null && eventsToExtract.HasValue() ? eventsToExtract.ParsedValue : (uint?) null;
            var offset = offSetKind != null && offSetKind.HasValue() ? offSetKind.ParsedValue : (OffsetKind?)null;

            var readConfigurationAsync = await ExtractorHelper.CreateTopicConfiguration(kafkerSettings,
                configName.Value(),
                brokers?.Value(),
                topicName?.Value(),
                events,
                offset);
            return readConfigurationAsync;
        }

        private static ServiceProvider CreateServiceProvider(KafkerSettings kafkerSettings, Action<IServiceCollection> addAdditionalServices = null)
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
                .AddSingleton<IConfigCommand, ConfigCommand>()
                .AddSingleton(PhysicalConsole.Singleton)
                .AddSingleton(kafkerSettings);

            addAdditionalServices?.Invoke(servicesCollection);

            var services = servicesCollection.BuildServiceProvider();

            return services;
        }


        public static IConfigurationRoot CreateConfiguration(string environment)
        {
            environment ??= "Development";
            var env = environment.ToLowerInvariant();
            var basePath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            var builder = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", true, true)
                .AddJsonFile($"appsettings.{env}.json", true, true);
            var configuration = builder.Build();

            return configuration;
        }
    }
}