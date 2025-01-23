using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-02 16:45:31 ngocta2
    /// Logger chinh cho he thong 6G
    /// tai day: define chung chung, chua ref vao lib nao cu the
    /// https://github.com/jignesht24/Aspnetcore/tree/master/Logging%20with%20.net%20core%202.0/Using%20Serilog/SerilogUsingFileSink
    /// https://github.com/serilog/serilog-sinks-file/issues/53 - At present no filename customization is supported - May 19, 2018    
    /// https://github.com/serilog/serilog-sinks-file
    /// https://github.com/serilog/serilog-sinks-async -  it is important to call Log.CloseAndFlush() or Logger.Dispose() when the application exits.
    /// 
    /// 
    /// 
    /// + Async : written to disk on the worker thread
    ///     Serilog.Sinks.Async
    /// 
    /// </summary>
    public interface ILogProvider
    {

    }
}
