using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{

	public class DataElement
	{
		public string HNXDSAll { get; set; }
	}

	public class EHnxListDS
	{
		public string Time { get; set; }
		public List<DataElement> Data { get; set; }
	}
}
