using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.FuDataService.Cache
{
	public class ETable
	{
		public string ASTOCKCODE { get; set; }
		public double ACEILPRICE { get; set; }
		public double AFLOORPRICE { get; set; }
		public double AREFPRICE { get; set; }
		public string ABASICCODE { get; set; }
		public int ABASICTYPE { get; set; }
		public string ASTARTDTE { get; set; }
		public string AENDDTE { get; set; }
		public double APRICEUNIT { get; set; }


		/// <summary>
		/// 2019-03-01 09:06:11 ngocta2
		/// tu them vao de luu data ma fu
		/// VN301903--> lưu vào db với key là VN30F1M
		/// </summary>
		public string FuSymbol { get; set; }		
		public DateTime EndDate { get; set; }
		/// <summary>
		/// EndDate-StartDate tinh theo day
		/// </summary>
		public double DateDiffInDays { get; set; }
	}
}
