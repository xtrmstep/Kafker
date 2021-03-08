using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface IConfigCommand
    {
        Task<int> InvokeAsync();
    }
}