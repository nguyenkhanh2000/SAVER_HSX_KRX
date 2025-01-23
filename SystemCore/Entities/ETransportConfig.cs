using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
	/// <summary>
	/// 2019-07-31 14:49:43 ngocta2
	/// cac config can thiet de transport data trong noi bo
	/// </summary>
	public class ETransportConfig
	{
		/// <summary>
		/// 2020-08-20 10:40:04 ngocta2
		/// </summary>
		public const string __SECTION_TRANSPORT_CONFIG = "TransportConfig";  // old la BaseTransportLib
		public const string __KEY_PROVIDER = "Provider";  // Provider
		public const string __KEY_TOKEN = "Token";  // CommandToken
		public const int __PROVIDER_ZMQ = 0;
		public const int __PROVIDER_REDIS = 1;

		public static string GetProviderName(int provider)
		{
			switch (provider)
			{
				case __PROVIDER_ZMQ: return "ZMQ";
				case __PROVIDER_REDIS: return "REDIS";
			}
			return "???";
		}

		/// <summary>
		/// 0 - zmq
		/// 1 - redis
		/// </summary>
		public int Provider { get; set; }

		/// <summary>
		/// thong tin ve cac pub
		/// sub can biet de connect vao cac pub do
		/// </summary>
		public List<EPub> Publishers { get; set; }

		/// <summary>
		/// thong tin tai phia sub
		/// </summary>
		public ESub Subscriber { get; set; }

		/// <summary>
		/// cac kenh de pub data tu feeder sang web server
		/// </summary>
		public EChannel Channels { get; set; }

		/// <summary>
		/// random string - trong config cua ApiService
		/// dung token moi dc set chuyen transport provider
		/// </summary>
		public string Token { get; set; }
	}

	public class EPub
	{
		/// <summary>
		/// phan biet cac pub 
		/// </summary>
		public string Name { get; set; }

		/// <summary>
		/// IP cua publisher, subscriber can biet de connect vao nhan data
		/// trong LAN nen dung luon IP, ko dung host name
		/// 10.26.2.33
		/// </summary>
		public string Ip { get; set; }

		/// <summary>
		/// Port cua publisher, subscriber can biet de connect vao nhan data
		/// trong LAN nen dung luon IP, ko dung host name
		/// 6666
		/// </summary>
		public int Port { get; set; }

		/// <summary>
		/// ten kenh de pub/sub data
		/// neu ten kenh de trang tuc la pub/sub tren nhieu kenh khac nhau, rat nhieu ten kenh
		/// da su dung de pub/sub voi ten kenh la ma ck. vay la khoang gan 1.8k kenh
		/// </summary>
		public string Channel { get; set; }

		/// <summary>
		/// The high water mark is a hard limit on the maximum number of outstanding messages 
		/// NetMQ shall queue in memory for any single peer that the specified socket is communicating with.
		/// If this limit has been reached the socket shall enter an exceptional state and depending 
		/// on the socket type, NetMQ shall take appropriate action such as blocking or dropping sent messages.
		/// </summary>
		public int HighWaterMark { get; set; }
	}

	public class ESub
	{
		/// <summary>
		/// http://10.26.7.20:8080/tfs/APP_FRONT/Stock6G/_wiki?pagePath=%2FFlow%2FTransport%2FBaseTransportLib
		/// </summary>
		public int CheckPortTimeout { get; set; }

		/// <summary>
		/// http://10.26.7.20:8080/tfs/APP_FRONT/Stock6G/_wiki?pagePath=%2FFlow%2FTransport%2FBaseTransportLib
		/// </summary>
		public int CheckPortInterval { get; set; }
	}

	/// <summary>
	/// 2021-10-06 11:02:07 ngocta2
	/// luu cac info kenh pub/sub vao day, doc lap voi feeder
	/// 
	///"Channels": {
	///			"HeartBeat": "HB_{Exchange}",
	///			"SnapshotQuote": "SQ_{Exchange}_{MarketID}_{BoardID}",
	///			"SnapshotIndex": "SI_{Exchange}"
	///		}
	/// </summary>
	public class EChannel
	{		
		public string HeartBeat { get; set; }
		public string SnapshotQuote { get; set; }
		public string SnapshotIndex { get; set; }
		public string SnapshotMarketStatus { get; set; }
	}
}
