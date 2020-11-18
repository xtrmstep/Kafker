using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Commands
{
    public interface IExtractCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string topic,Dictionary<string,string> argumentList);
    }
}