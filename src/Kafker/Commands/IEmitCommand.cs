using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface IEmitCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string topic, string fileName);
    }
}