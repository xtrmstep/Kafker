using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface IExtractCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string map);
    }
}