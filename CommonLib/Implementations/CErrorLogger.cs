using CommonLib.Interfaces;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading;
using SystemCore.Temporaries;

namespace CommonLib.Implementations
{
    /// <summary>
    /// 2019-01-03 14:38:15 ngocta2
    /// class ghi log error
    /// </summary>
    public class CErrorLogger : CBaseLogger, IErrorLogger
    {
        private const string __TEMPLATE = @"=================
Message     = {0}
StackTrace  = {1}
TargetSite  = {2}";


        private const string __TEMPLATE_CONTEXT = @"=================
ContextId   = {0} ({1}) [{2}]
LastContext = {3}
Message     = {4}
StackTrace  = {5}
TargetSite  = {6}";

        private const string __LOG_FOLDER = "ERROR";

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="configuration"></param>
        public CErrorLogger(IConfiguration configuration, bool randomFileName = false)
            : base(configuration, __LOG_FOLDER, randomFileName)
        {
        }

        /// <summary>
        /// 2019-01-03 15:55:18 ngocta2
        /// ghi log error
        /// </summary>
        /// <param name="ex"></param>
        public void LogError(Exception ex)
        {   
            this._logger.Error(
                __TEMPLATE,
                ex.Message,
                ex.StackTrace.TrimStart(),
                ex.TargetSite);            
        }


        /// <summary>
        /// 2019-01-08 14:37:47 ngocta2
        /// log them contextData de chi tiet hon cac nguyen nhan gay ra error
        /// 2019-01-24 11:21:42 ngocta2
        /// them managedThreadId de phan biet code run giua cac thread khac nhau
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="executionContext"> id + data </param>
        public void LogErrorContext(Exception ex, TExecutionContext ec)
        {
            // 1. log vao file cua Serilog
            this._logger.Error(
                __TEMPLATE_CONTEXT,
                ec.Id,
                ThreadId,
                TaskId,
                ec.LastContext + $" ; ec.Data={ec.Data}",
                ex.Message,
                ex.StackTrace.TrimStart(),
                ex.TargetSite);

            // 2. log vao buffer
            WriteBufferMid(ec, ex.Message, true);

        }
    }
}
