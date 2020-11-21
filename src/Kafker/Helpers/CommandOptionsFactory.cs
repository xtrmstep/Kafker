using System.ComponentModel.DataAnnotations;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;
using McMaster.Extensions.CommandLineUtils.Validation;

namespace Kafker.Helpers
{
    public static class CommandOptionsFactory
    {
        public static CommandOption ConfigOption(CommandLineApplication p)
        {
            return p.Option("-cfg|--config <CONFIG>", "Topic configuration name", CommandOptionType.SingleValue);
        }
        
        public static CommandArgument ConfigArgument(CommandLineApplication p)
        {
            return p.Argument("config", "Topic configuration name").IsRequired();
        }
        
        public static CommandOption BrokersOption(CommandLineApplication p)
        {
            return p.Option("-b|--brokers <BROKER>", "Kafka brokers", CommandOptionType.MultipleValue);
        }
        
        public static CommandOption TopicOption(CommandLineApplication p)
        {
            return p.Option("-t|--topic <TOPIC>", "Kafka topic name", CommandOptionType.SingleValue);
        }
        
        public static CommandOption<OffsetKind> OffsetOption(CommandLineApplication p)
        {
            var offSetKind = p.Option<OffsetKind>("-o|--offset <OFFSET>", "Offset in the topic to read events from", CommandOptionType.SingleValue);
            offSetKind.Validators.Add(new OffsetKindValidator());
            return offSetKind;
        }

        public static CommandOption<uint> NumberOption(CommandLineApplication p)
        {
            return p.Option<uint>("-n|--number <NUMBER>", "Maximum number of events (0 - infinite)", CommandOptionType.SingleValue);
        }
        
        public static CommandOption PreserveOption(CommandLineApplication p)
        {
            return p.Option("-p|--preserve <PRESERVE>", "Preserve time intervals when emitting events", CommandOptionType.NoValue);
        }
        
        public static CommandArgument FileArgument(CommandLineApplication p)
        {
            return p.Argument("file", "Snapshot file name (.DAT)").IsRequired();
        }

        public class OffsetKindValidator : IOptionValidator
        {
            public ValidationResult GetValidationResult(CommandOption option, ValidationContext context)
            {
                // This validator only runs if there is a value
                if (!option.HasValue()) return ValidationResult.Success;
                var val = option.Value();

                if (val != "earliest" && val != "latest")
                {
                    return new ValidationResult($"The value for --{option.LongName} must be 'red' or 'blue'");
                }

                return ValidationResult.Success;
            }
        }
    }
}