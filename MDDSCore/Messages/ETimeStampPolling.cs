using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=32; name=Time Stamp (Polling); type=MV; Online</i></para>
	/// <para><b>Phân phối thông tin thời gian để kiểm tra đường mạng vật lý.</b></para>   
	/// <para>Thông tin thống kê này được tạo ra định kỳ trong giờ giao dịch cung cấp Top N chứng khoán có khối lượng giao dịch lớn nhất.Dữ liệu sớm nhất được tạo ra sau đợt khớp lệnh định kỳ mở cửa</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ETimeStampPolling: EBase
    {

		/// <summary>
		/// MV =  Time Stamp (Polling)
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_TIME_STAMP_POLLING;



		/// <summary>
		/// 2020-04-28 14:35:22 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60, Order = 8)]
		public string TransactTime { get; set; }

		
		
		// test commit push
	}
}
