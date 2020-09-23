using System;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Kafker.Csv
{
    public interface ICsvFileWriter : IDisposable
    {
        Task WriteAsync(CancellationToken cancellationToken, JObject json);
    }
}