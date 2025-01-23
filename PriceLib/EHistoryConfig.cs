using System;
using System.Collections.Generic;
using System.Text;

namespace PriceLib
{
    public class EHistoryConfig
	{
		public const string __LOG_SQL_FILENAME = "PriceLib_Hist.js";
		public const string __SECTION_HISTORYCONFIG = "HistoryConfig";

		/// <summary>
		/// "Server=10.26.7.31\MSSQLSERVER2019,1435; Database=STCADAPTER;User Id=sa;Password=fpts@123; MultipleActiveResultSets=true",
		/// </summary>
		public string ConnectionMssqlDbStcadapter { get; set; }

		/// <summary>
		/// "Server=10.26.7.31\MSSQLSERVER2019,1435; Database=StockInfoStore;User Id=sa;Password=fpts@123; MultipleActiveResultSets=true",
		/// </summary>
		public string ConnectionMssqlDbStockInfoStore { get; set; }

		/// <summary>
		/// "Data Source=tradingtest;User Id=authen;Password=authen1234"
		/// </summary>
		//public string ConnectionOracle { get; set; }

		/// <summary>
		/// [10.26.248.64].STCADAPTER.dbo.prc_S5G_HIST_GET_INDEX
		/// </summary>
		public string SpStcadapter64HistGetIndex { get; set; }

		/// <summary>
		/// [10.26.248.64].StoredProcedure.dbo.prc_S5G_HIST_GET_STOCK_PRICE
		/// </summary>
		public string SpStcadapter64HistGetStockPrice { get; set; }

		/// <summary>
		/// [10.26.248.64].StoredProcedure.dbo.prc_S5G_HIST_GET_STOCK_ORDER
		/// </summary>
		public string SpStcadapter64HistGetStockOrder { get; set; }

		/// <summary>
		/// [10.26.248.64].StoredProcedure.dbo.prc_S5G_HIST_GET_STOCK_FOREGIN_NM
		/// </summary>
		public string SpStcadapter64HistGetStockForeginNM { get; set; }

		/// <summary>
		/// [10.26.248.64].StoredProcedure.dbo.prc_S5G_HIST_GET_STOCK_FOREGIN_PT
		/// </summary>
		public string SpStcadapter64HistGetStockForeginPT { get; set; }
	}
}
