using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{

	/// <summary>
	//{
	//  "TradingDateList": [
	//    "20190228",
	//    "20190301",
	//    "20190304",
	//    "20190305",
	//    "20190306",
	//    "20190307"
	//  ],
	//  "GetSymbolListURL": "http://tradingview.fpts.com.vn/getlistcode?s=3",
	//  "GetRealtimeHistDataURL": "http://tradingview.fpts.com.vn/getintradaydata?from={trading_date}&to={trading_date}&stock={symbol}",
	//  "ZKeyRealtimeTemplate": "INTRADAY:S6G__{symbol}",
	//  "ThreadMax": "30",
	//  "InsertOnly": "1"
	//}
	/// </summary>
	public class ETAChartRealtimeHistConfig
	{
		public List<string> TradingDateList { get; set; }
		public string GetSymbolListURL { get; set; }
		public string GetRealtimeHistDataURL { get; set; }
		public string ZKeyRealtimeTemplate { get; set; }
		public int ThreadMax { get; set; }
		public int InsertOnly { get; set; }
	}
}
