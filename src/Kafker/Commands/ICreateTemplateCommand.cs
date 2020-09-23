using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface ICreateTemplateCommand
    {
        Task<int> InvokeAsync(CancellationToken cancellationToken, string templateName);
    }
}