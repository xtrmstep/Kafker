using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public interface IExtractCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, KafkaTopicConfiguration configuration);
    }
}