using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface IConvertCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string fileName, string topic);
    }
}