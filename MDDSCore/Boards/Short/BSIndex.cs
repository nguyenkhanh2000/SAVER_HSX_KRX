using MDDSCore.Boards.Full;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Boards.Short
{
	/// <summary>
	/// 2020-10-02 08:52:10 ngocta2
	/// index data
	/// struct ngan gon dung de serialize data truoc khi send qua zero mq tu feeder den web server
	/// </summary>
	public class BSIndex
	{
		public string m { get; set; }
		public string ts { get; set; }
		public string tt { get; set; }
		public string i { get; set; }
		public double v { get; set; }
		public double c { get; set; }
		public double cp { get; set; }
		public long tq { get; set; }
		public double tv { get; set; }
		public long nmtq { get; set; }
		public double nmtv { get; set; }
		public long pttq { get; set; }
		public double pttv { get; set; }
		public int cc { get; set; }
		public int uc { get; set; }
		public int nc { get; set; }
		public int dc { get; set; }
		public int? fc { get; set; }

		// ===============================================================

		public BSIndex() { }
		public BSIndex(BFIndex entity)
		{
			this.m    = entity.MarketID;
			this.ts   = entity.TradingSessionID;
			this.tt   = entity.TransactTime;
			this.i    = entity.Index;
			this.v    = entity.Value;
			this.c    = entity.Change;
			this.cp   = entity.ChangePercent;
			this.tq   = entity.TotalQuantity;
			this.tv   = entity.TotalValue;
			this.nmtq = entity.NMTotalQuantity;
			this.nmtv = entity.NMTotalValue;
			this.pttq = entity.PTTotalQuantity;
			this.pttv = entity.PTTotalValue;
			this.cc   = entity.CeilingCount;
			this.uc   = entity.UpCount;
			this.nc   = entity.NochangeCount;
			this.dc   = entity.DownCount;
			this.fc   = entity.FloorCount;
		}
	}
}
