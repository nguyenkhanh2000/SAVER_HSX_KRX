using CommonLib.Interfaces;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace CommonLib.Implementations
{
    /// <summary>
    /// 2019-01-04 14:58:37 ngocta2
    /// class log script truy van SQL Server / Oracle / Redis
    /// </summary>
    public class CSqlLogger: CBaseLogger, ISqlLogger
    {
        private const string __TEMPLATE = @"=================
Source  = {0}
Data    = {1}";
        
        private const string __TEMPLATE_CONTEXT = @"=================
Source  = {0} => {1} ({2}) [{3}]
Data    = {4}";

		private const string __TYPE_FOLDER = __TYPE_FOLDER_SQL;//"SQL";

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="configuration"></param>
        public CSqlLogger(IConfiguration configuration, bool randomFileName = false)
            : base(configuration, __TYPE_FOLDER, randomFileName)
        {
        }

        /// <summary>
        /// 2019-01-03 15:55:18 ngocta2
        /// ghi log error
        /// </summary>
        /// <param name="ex"></param>
        public void LogSql(string data)
        {
            this._logger.Information(__TEMPLATE, GetDeepCaller(), data);
        }

        /// <summary>
        /// 2019-01-24 11:21:42 ngocta2
        /// them managedThreadId de phan biet code run giua cac thread khac nhau
        /// </summary>
        /// <param name="data"></param>
        /// <param name="ec"></param>
        public void LogSqlContext(TExecutionContext ec, string data)
        {
            this._logger.Information(
                __TEMPLATE_CONTEXT,
                GetDeepCaller(),
                ec.Id,
                ThreadId,
                TaskId,
                data);

            // 2. log vao buffer
            WriteBufferMid(ec, data, true);
        }

		/// <summary>
		/// 2019-09-26 09:24:34 ngocta2
		/// ko ghi log 1 file to ma chia nho: tao sub folder tung ngay, luu cac file log co name khac nhau
		/// </summary>
		/// <param name="executionContext"></param>
		/// <param name="fileName"></param>
		/// <param name="data"></param>
		public void LogSqlSub(TExecutionContext ec, string fileName, string data)
		{
			LogSub(__TEMPLATE_CONTEXT, __TYPE_FOLDER, GetDeepCaller(), ec, fileName, data);
		}
	}
}
