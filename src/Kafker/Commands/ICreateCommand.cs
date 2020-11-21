using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface ICreateCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string configName);
    }
}