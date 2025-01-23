using BaseSaverLib.Implementations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace BaseSaverLib.Interfaces
{
    public interface IMDDSHandler
    {
        //Task<EResponseResult> UpdateBulk(string dataBlock);
        Task<EBulkScript> ProcessMessage(string msgType, string rawData, ProcessStateRedis processStateRedis);
        bool BuildScriptSQL(string[] arrMsg);  
    }
}

