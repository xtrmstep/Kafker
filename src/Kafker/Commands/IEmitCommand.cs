using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;

namespace Kafker.Commands
{
    public interface IEmitCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, KafkaTopicConfiguration kafkaTopicConfiguration, string fileName);
    }
}