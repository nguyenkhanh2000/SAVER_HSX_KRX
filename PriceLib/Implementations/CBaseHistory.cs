using CommonLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace PriceLib.Implementations
{
    public abstract class CBaseHistory
	{
		// const cho cac function DAL (exec sp)

		protected const string __CENTER       = "Center";
		protected const string __TIME         = "Time";
		protected const string __BEGINDATE    = "BeginDate";
		protected const string __ENDDATE      = "EndDate";
		protected const string __SELECTEDPAGE = "SelectedPage";
		protected const string __PAGESIZE     = "PageSize";
		protected const string __CODE         = "Code";
		protected const string __TYPE         = "Type";
		protected const string __DATE         = "Date";

		// var
		protected readonly IS6GApp _cS6GApp;
        protected readonly EHistoryConfig _eHistoryConfig;

		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="s6GApp"></param>
		public CBaseHistory(IS6GApp s6GApp, EHistoryConfig eHistoryConfig)
		{
			this._cS6GApp = s6GApp;
			this._eHistoryConfig = eHistoryConfig;
		}

		/// <summary>
		/// destructor
		/// </summary>
		~CBaseHistory()
		{

		}
	}
}
