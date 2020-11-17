using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Helpers
{
    public interface IEventsEmitter
    {
        Task<int> EmitEvents(CancellationToken cancellationToken,string filename, string topic);
    }
}