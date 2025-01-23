using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Temporaries;

namespace SystemCore.Interfaces
{
	/// <summary>
	/// 2019-03-06 17:16:53 ngocta2
	/// </summary>
	public interface ILogInfo : ILog
	{
		/// <summary>
		/// khi log info de biet app dang chay den dau
		/// </summary>
		/// <param name="data"></param>
		void LogInfo(string data);


		/// <summary>
		/// log info, log ngay ma ko can quan tam debug flag
		/// </summary>
		/// <param name="executionContext"></param>
		/// <param name="data"></param>
		void LogInfoContext(TExecutionContext executionContext, string data);
	}
}
