using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Helpers
{
    public interface IEmitter
    {
        Task<int> EmitEvents(CancellationToken cancellationToken,string filename, string topic);
    }
}