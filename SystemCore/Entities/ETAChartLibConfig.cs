using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
	public class RedisInfo
	{
		public string Ip { get; set; }
		public int Port { get; set; }
		public int Db { get; set; }
		/// <summary>
		/// 10.26.7.84:6379?db=4
		/// "server1:6379,server2:6379" => master/slave 
		/// </summary>
		public string ConnectionString { get; set; }
		public string Channel { get; set; }
	}

	//public class Source
 //   {
 //       public string Ip { get; set; }
 //       public int Port { get; set; }
 //       public string HnxChannel { get; set; }
 //   }

 //   public class Destination
 //   {
 //       public string Ip { get; set; }
 //       public int Port { get; set; }
 //       public int Db { get; set; }
 //       public string ConnectionString { get; set; }
 //   }

	public class DeleteOldData
	{
		public string Type { get; set; }
		public int TradingDayMax { get; set; }
	}

	public class Redis
    {
        public RedisInfo Source { get; set; }
        public RedisInfo Destination { get; set; }
    }

    public class ETAChartLibConfig
    {
        /// <summary>
        /// 1-HSX(file)
        /// 2-HNX(message)
        /// </summary>
        public int DataSource { get; set; }        
        /// <summary>
        /// path ghi log
        /// </summary>
        public string LogDir { get; set; }

		/// <summary>
		/// thoi gian exit app
		/// "ExitAppTime": "17:00",
		/// </summary>
		public string ExitAppTime { get; set; }

		/// <summary>
		/// thoi gian make hist
		/// "MakeHistTime": "15:30",
		/// </summary>
		public string MakeHistTime { get; set; }
		
		/// <summary>
		/// thoi gian make hist Adjusted (vuot gia)
		/// "MakeHistAdjustedTime": "23:59",
		/// rat nhieu data nen phai chay luc dem
		/// </summary>
		public string MakeHistAdjustedTime { get; set; }

		/// <summary>
		/// su dung cho data HNX : FU + co so
		/// chu ky tinh toan, su dung data dang nam trong dic
		/// </summary>
		public int CalculateInterval { get; set; }
		/// <summary>
		/// 2019-03-01 09:55:10 ngocta2
		/// "VN30,VN30F^1M,2M,1Q,2Q|VN100,VN100F^1M,2M,1Q,2Q"
		/// http://s11.api.ezfutures.fpts.com.vn/api/future/v1/cache/stock_info
		/// return
		///  {
		///  "ASTOCKCODE": "VN30F1909",
		///  "ACEILPRICE": 958.7,
		///  "AFLOORPRICE": 833.3,
		///  "AREFPRICE": 896,
		///  "ABASICCODE": "VN30",
		/// "ABASICTYPE": 1,
		///  "ASTARTDTE": "18/01/2019",
		///  "AENDDTE": "19/09/2019",
		///  "APRICEUNIT": 0.1
		///  }
		///  sort list asending theo dateDiffInDay cua EndDate-Now
		///  ABASICCODE => so sanh voi "VN30", neu = thi lay prefix = VN30F
		///  suffix = 1M,2M ..... tang dan theo index cua cac ele trong sortedList
		/// </summary>
		public string FuPrefixSuffixList { get; set; }
		/// <summary>
		/// URL request vao api fuDataService lay data list stock info de tinh ra fu symbol
		/// </summary>
		public string FuCacheStockInfoURL { get; set; }
		/// <summary>
		/// URL lay du lieu hist today de make hist vao redis
		/// </summary>
		public string FuGetHistTodayURL { get; set; }

		/// <summary>
		/// lay danh muc ngay nghi trong qua khu, 2 thang gan nhat ... dung de xoa data cu hon 20 ngay
		/// </summary>
		public string FuGetNonworkingdayURL { get; set; }


		/// <summary>
		/// lay danh ma gom: ma co so, ma phai sinh, index
		/// http://tradingview.fpts.com.vn/getlistcode
		/// http://tradingview.fpts.com.vn/getlistcode?s=3 de giam bot so luong return
		/// </summary>
		public string FuGetCodeListURL { get; set; }


		/// <summary>
		/// lay data gia vuot, nhieu data nen phai tach nho, moi lan chi lay 1 ma, time from 2007 to now
		/// http://tradingview.fpts.com.vn/api/makehist/getdata?from=20070101&stock=(symbol)
		/// </summary>
		public string FuGetHistAdjustedURL { get; set; }

		/// <summary>
		/// lay list cac short ip , hien tai chi co 11 va 12 >> "11,12"
		/// </summary>
		public string FuShortIpList { get; set; }

		/// <summary>
		/// chu ky xu ly VNX HSX
		/// </summary>
		public int ProcessHSXIndexInterval { get; set; }
        /// <summary>
        /// chu ky xu ly co so HSX
        /// </summary>
        public int ProcessHSXQuoteInterval { get; set; }
		/// <summary>
		/// chu ky xu ly chi so VnIndex
		/// </summary>
		public int ProcessHSXVniInterval { get; set; }
		/// <summary>
		/// cac file VNX se read,process
		/// </summary>
		public string HSXIndexList { get; set; }        
        /// <summary>
        /// path cua file INI (stock5G) -> luu cac config can thiet de run code read cac file binary cua HSX
        /// D:\QuoteFeeder.ini
        /// </summary>
        public string IniPath { get; set; }
		/// <summary>
		/// array cac thong tin chi tiet so ngay can luu max cua tung loai chung khoan
		/// </summary>
		public List<DeleteOldData> DeleteOldData { get; set; }
		/// <summary>
		/// cac info lien quan REDIS
		/// </summary>
		public Redis Redis { get; set; }

		/// <summary>
		/// D:\jsonA.js
		/// </summary>
		public string DataSourcePath { get; set; }
	}
}
