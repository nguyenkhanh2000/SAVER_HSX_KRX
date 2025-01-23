using CommonLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.SharedKernel;

namespace CommonLib.Implementations
{
    public abstract class CBaseS6GApp: CInstance
    {
        // vars
        protected readonly IS6GApp _app;

		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="app"></param>
		public CBaseS6GApp(IS6GApp app)
		{
			this._app = app;
		}

		/// <summary>
		/// destructor
		/// </summary>
		~CBaseS6GApp()
		{
			try
			{
				// free				
			}
			catch (Exception ex)
			{
				 this._app.ErrorLogger.LogError(ex);
			}
		}
	}
}
