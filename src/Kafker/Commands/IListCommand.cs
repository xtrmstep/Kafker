using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface IListCommand
    {
        Task<int> InvokeAsync();
    }
}