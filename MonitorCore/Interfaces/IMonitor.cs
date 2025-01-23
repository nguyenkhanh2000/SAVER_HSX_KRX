using MonitorCore.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace MonitorCore.Interfaces
{
    /// <summary>
    /// 2019-01-15 09:39:58 ngocta2
    /// interface monitor cho cac monitor client (tat ca project se dung)
    /// </summary>
    public interface IMonitor
    {
        string GetAppName(AppList appList);
        bool SendStatusToMonitor(AppList appList, string statusData);
    }
}
