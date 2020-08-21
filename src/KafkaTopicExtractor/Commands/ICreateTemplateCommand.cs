using System.Threading;
using System.Threading.Tasks;

namespace KafkaTopicExtractor.Commands
{
    public interface ICreateTemplateCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string templateName);
    }
}