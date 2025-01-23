using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.TAChart.Entities
{
	/// <summary>
	/// 2019-03-05 10:20:33 ngocta2
	/// luu struct cua json response tu URL 
	/// https://ezfutures.fpts.com.vn/chart3api/api/makehist/getdata?date=20190304
	/// https://ezfutures.fpts.com.vn/chart3api/api/makehist/getdata
	/// </summary>
	public class EJsonHist
	{
		public string symbol { get; set; }
		public EDataBlock data { get; set; }
		public string score { get; set; }
	}
}
