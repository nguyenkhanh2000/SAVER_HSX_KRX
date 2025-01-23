using Microsoft.Extensions.Configuration;
using MonitorCore.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Interfaces;

namespace CommonLib.Interfaces
{
    /// <summary>
    /// 2019-01-03 14:33:47 ngocta2
    /// 3 instance logger ghi log Error + SQL + Debug
    /// 1 instance monitorClient send data cho server
    /// tat ca app thuoc Host group deu co 1 instance cua IS6GApp
    /// </summary>
    public interface IS6GApp: IInstance
    {
        IErrorLogger    ErrorLogger     { get; }
        ISqlLogger      SqlLogger       { get; }
		IInfoLogger		InfoLogger		{ get; }
		IDebugLogger    DebugLogger     { get; }
        ICommon         Common          { get; }
        IConfiguration  Configuration   { get; }
        IMonitor        Monitor         { get; }
        IHandCode       HandCode        { get; }
    }
}
