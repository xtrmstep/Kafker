using System.Threading;
using System.Threading.Tasks;

namespace Kafker.Commands
{
    public interface IConfigCommand
    {
        Task<int> ShowConfigurationAsync();
        Task<int> SetConfigurationFolderAsync(string value);
        Task<int> SetDestinationFolderAsync(string value);
    }
}