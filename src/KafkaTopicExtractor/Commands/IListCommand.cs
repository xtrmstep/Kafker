using System.Threading;
using System.Threading.Tasks;

namespace KafkaTopicExtractor.Commands
{
    public interface IListCommand
    {
        Task<int> InvokeAsync();
    }
}