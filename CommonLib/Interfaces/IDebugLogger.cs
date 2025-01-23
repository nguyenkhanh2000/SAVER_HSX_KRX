using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Interfaces;

namespace CommonLib.Interfaces
{
    /// <summary>
    /// 2019-01-03 14:29:28 ngocta2
    /// </summary>
    public interface IDebugLogger: ISerilogProvider, ILogDebug, IDisposable, IInstance
    {

    }
}
