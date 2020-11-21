using System.Threading;
using System.Threading.Tasks;
using Kafker.Configurations;

namespace Kafker.Commands
{
    public interface IConvertCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string fileName, KafkaTopicConfiguration topicConfiguration);
    }
}