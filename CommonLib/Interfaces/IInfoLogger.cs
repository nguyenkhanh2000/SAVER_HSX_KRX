using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Interfaces;

namespace CommonLib.Interfaces
{
	/// <summary>
	/// 2019-03-06 17:18:56 ngocta2
	/// </summary>
	public interface IInfoLogger : ISerilogProvider, ILogInfo, IDisposable, IInstance
	{
		string FullLogPath
		{
			get;
		}

		string LogDirPath
		{
			get;
		}

		string LogRootPath
		{
			get;
		}

		
	}
}
