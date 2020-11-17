using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Kafker.Configurations;
using McMaster.Extensions.CommandLineUtils;

namespace Kafker.Helpers
{
    public class ConfigAdministrator
    {
        
        
        public static async Task OverrideArgumentList(Dictionary<string,string> argumentList,string fileName)
        {
            if (!File.Exists(fileName))
            {
                fileName = Path.Combine(Environment.CurrentDirectory, $"{fileName}.cfg");
                if (!File.Exists(fileName))
                {
                    await Console.Error.WriteAsync($"Error: Cannot find the file: {fileName}");
                }
            }
            
        }
    }
}