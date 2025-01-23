using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Temporaries;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-03 11:09:39 ngocta2
    /// + Log Error => folder ERROR
    ///     log tat ca error details
    /// </summary>
    public interface ILogError: ILog
    {
        /// <summary>
        /// ghi chi tiet log error vao file (async)
        /// </summary>
        /// <param name="ex"></param>
        void LogError(Exception ex);

        /// <summary>
        /// co du data de debug
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="executionContext"></param>
        void LogErrorContext(Exception ex, TExecutionContext  executionContext);
    }
}
