using Newtonsoft.Json;
using StockCore.TAChart.Enums;
using System;
using System.Collections.Generic;
using System.Text;
//using ZeroFormatter;

namespace StockCore.TAChart.Entities
{
    /// <summary>
    /// 2019-01-16 09:37:02 ngocta2
    /// entity chi luu cac field can thiet de tinh ra TA chart data
    /// </summary>
    //[ZeroFormattable]
	[JsonObject(MemberSerialization.OptIn)]
	public class EJsonADetail
    {
        /// <summary>
        /// time tai server cua so HNX        
        /// // "f399": "13:02:39"   /// Thời gian theo định dạng HH:mm:ss
        /// hiện t chỉ có HH:mm ==> cần bổ sung thành yyyy-MM-dd HH:mm
        /// </summary>
        //[Index(0)]
        [JsonProperty(PropertyName = "ST")]
        public virtual string ServerTime { get; set; }

        /// <summary>
        /// // "f388": "20190123"   /// Tradingdate date Ngày giao dịch hiện tại theo định dạng yyyyMMdd
        /// </summary>
        //[Index(1)]
        [JsonProperty(PropertyName = "SD")]
        public virtual string ServerDate { get; set; }

        /// <summary>
        /// // "f31": "12000",
        /// Giá khớp gần nhất của GD khớp lệnh lô chẵn
        /// </summary>
        //[Index(2)]
        [JsonProperty(PropertyName = "MP")]
        public virtual double MatchPrice { get; set; }

        /// <summary>
        /// // "f32": "500",
        /// KL khớp gần của GD khớp lệnh lô chăn
        /// gia su co 3 lenh khop lien tuc
        /// Price   Qtty    Total
        /// 1100    1       11
        /// 1200    2       13
        /// 1300    3       16
        /// neu row 2 bi mat do chu ky update qua nhanh thi v trong block cua phut do bi mat 2 
        /// vi vay phai ra cot MatchQuantityEx de tinh khoi luong khop dua vao tong KL
        /// </summary>
        //[Index(3)]
        [JsonProperty(PropertyName = "MQ")]
        public virtual long MatchQuantity { get; set; }

        /// <summary>
        /// // cot tu tinh, ko co trong spec IG
        /// MatchQuantityEx = TotalVolumeTraded (hien tai) - TotalVolumeTraded ( truoc do )
        /// </summary>
        //[Index(4)]
        [JsonProperty(PropertyName = "MQE")]
        public virtual long MatchQuantityEx { get; set; }

        /// <summary>
        /// Tổng KL giao dịch của GD khớp lệnh và thỏa thuận (lô chẵn và lẻ)
        /// // "f387": "12118",
        /// </summary>
        //[Index(5)]
        [JsonProperty(PropertyName = "TVT")]
        public virtual long TotalVolumeTraded { get; set; }

        /// <summary>
        /// Tổng khối lượng giao dịch thông thường của GD khớp lệnh lô chẵn
        /// // "f391": "500",
        /// </summary>
        //[Index(6)]
        [JsonProperty(PropertyName = "NMQ")]
        public virtual long NM_TotalTradedQtty { get; set; }

		/// <summary>
		/// 2019-02-25 10:40:46 ngocta2
		/// hidden field de nhap, tinh toan ..... ko output ra khi SerializeObject
		/// cong thuc la lay LastVol cua phut nay 10:09 tru di lastVol cua phu truoc 10:08
		/// nhung khi removeElement het roi thi chi giu cac elemnt cua phut hien tai 10:09
		/// can 1 field de luu lai lastVol cua phut truoc 10:08 ... chinh la InitNM la lastVol cua 10:08
		/// </summary>
		[JsonProperty(PropertyName = "INM")]
		public virtual long InitNM { get; set; }

		/// <summary>
		/// ko serialized field nay: loai chung khoan
		/// </summary>
		public StockTypes StockType { get; set; }

		/// <summary>
		/// 2019-07-05 15:37:21 ngocta2
		/// f137
		/// them field nay de lam UpdateRealtimeHistory
		/// </summary>
		[JsonProperty(PropertyName = "OP")]
		public virtual double OpenPrice { get; set; } = 0; // default 

		/// <summary>
		/// 2019-07-05 15:37:21 ngocta2
		/// f266
		/// them field nay de lam UpdateRealtimeHistory
		/// </summary>
		[JsonProperty(PropertyName = "HP")]
		public virtual double HighestPice { get; set; } = 0; // default 

		/// <summary>
		/// 2019-07-05 15:37:21 ngocta2
		/// f2661
		/// them field nay de lam UpdateRealtimeHistory
		/// </summary>
		[JsonProperty(PropertyName = "LP")]
		public virtual double LowestPrice { get; set; } = 0; // default 


        /// <summary>
        /// fix open vni sai, ko can serialize
        /// </summary>
        public virtual double UpVolume { get; set; } = 0; // default 

        /// <summary>
        /// fix open vni sai, ko can serialize
        /// </summary>
        public virtual double NoChangeVolume { get; set; } = 0; // default 

        /// <summary>
        /// fix open vni sai, ko can serialize
        /// </summary>
        public virtual double DownVolume { get; set; } = 0; // default 
    }
}
