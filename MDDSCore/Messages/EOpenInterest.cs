using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=19; name=Open Interest; type=MA; Batch</i></para>
	/// <para><b>Phân phối thông tin open interest nhận từ VSD khi thị trường đóng cửa.</b></para>   
	/// <para>Thông tin này được cung cấp vào đầu ngày giao dịch, cho biết tổng số lượng hợp đồng Quyền chọn đang còn lưu hành, chưa được thanh lý.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EOpenInterest :EBase
    {
		/// <summary>
		/// MA = Open Interest
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_OPEN_INTEREST;

        public EOpenInterest(string rawData) : base(rawData)
        {
        }

        /// <summary>
        /// 2019-11-28 14:58:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30001; required=Y; format=String; length=3</i></para>
        /// <para><b>ID xác định các thị trường</b></para>
        /// <para>
        ///  Sở giao dịch chứng khoán Tp Hồ Chí Minh <br></br>
        /// - STO: Thị trường cổ phiếu<br></br> 
        /// - BDO: Thị trường trái phiếu<br></br> 
        /// - RPO: Thị trường Repo
        /// </para>
        /// <para>
        ///  Sở giao dịch chứng khoán Hà Nội<br></br>
        /// - STX: Thị trường cổ phiếu<br></br> 
        /// - BDX: Thị trường trái phiếu chính phủ<br></br> 
        /// - DVX: Thị trường phái sinh
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30001 , Order = 8)]
		public string MarketID { get; set; }

		/// <summary>
		/// 2019-12-11 10:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order = 9)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=75 ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày giao dịch. Định dạng (YYYYMMDD)</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_75 , Order = 10)]
		public string TradeDate { get; set; }

		/// <summary>
		/// 2019-12-11 10:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30540 ; required=Y; format=Number; length=10</i></para>
		/// <para><b>Khối lượng của hợp đồng phái sinh chưa thực hiện</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30540 , Order = 11)]
		public long OpenInterestQty { get; set; }


	}
}
