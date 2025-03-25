using BaseSaverLib.Implementations;
using MDDSCore.Messages;
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
        Task<bool> BuildScriptSQL(string[] arrMsg);
        void ProcessAndEnqueueMessage(string strMessage);
        Task TimerProc_GroupREDIS();
        Task TimerProc_GroupSQL();
        Task TimerProc_GroupORACLE();
        Task ProcessDataRedis(EPrice objMsg);

    }
}

