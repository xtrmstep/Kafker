using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;

namespace Kafker.Helpers
{
    public interface IEventsEmitter
    {
        Task<int> EmitEvents(CancellationToken cancellationToken, KafkaTopicConfiguration cfg, string fileName);
    }
}