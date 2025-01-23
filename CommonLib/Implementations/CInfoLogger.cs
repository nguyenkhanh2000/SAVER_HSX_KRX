using CommonLib.Interfaces;
using Microsoft.Extensions.Configuration;
using System.Threading;
using SystemCore.Temporaries;

namespace CommonLib.Implementations
{
	/// <summary>
	/// 2019-01-04 14:58:37 ngocta2
	/// class log script truy van SQL Server / Oracle / Redis
	/// </summary>
	public class CInfoLogger : CBaseLogger, IInfoLogger
	{
		private const string __TEMPLATE = @"=================
Source  = {0}
Data    = {1}";

		private const string __TEMPLATE_CONTEXT = @"{0} = {1} ({2}) [{3}] <<{4}>> => {5}";

		private const string __TYPE_FOLDER = "INFO";

		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="configuration"></param>
		public CInfoLogger(IConfiguration configuration, bool randomFileName = false)
			: base(configuration, __TYPE_FOLDER, randomFileName)
		{
		}

		/// <summary>
		/// 2019-01-03 15:55:18 ngocta2
		/// ghi log error
		/// </summary>
		/// <param name="ex"></param>
		public void LogInfo(string data)
		{
			this._logger.Information(__TEMPLATE, GetDeepCaller(), data);
		}

		/// <summary>
		/// 2019-01-24 11:21:42 ngocta2
		/// them managedThreadId de phan biet code run giua cac thread khac nhau
		/// </summary>
		/// <param name="data"></param>
		/// <param name="ec"></param>
		public void LogInfoContext(TExecutionContext ec, string data)
		{
			this._logger.Information(
				__TEMPLATE_CONTEXT,
				GetDeepCaller(),				
				data,
				ThreadId,
				TaskId,
				ec.ElapsedMilliseconds,
				ec.Id);

			// 2. log vao buffer
			WriteBufferMid(ec, data, true);
		}
	}
}
