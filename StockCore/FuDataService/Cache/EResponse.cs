using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.FuDataService.Cache
{
	/// <summary>
	/// 2019-03-01 08:57:00 ngocta2
	/// response tu fu data service api (tro host)
	/// http://s11.api.ezfutures.fpts.com.vn/api/future/v1/cache/stock_info
	/// http://s18.api.ezfutures.fpts.com.vn/api/future/v1/cache/stock_info
	/// </summary>
	public class EResponse
	{
		public int Code { get; set; }
		public string Message { get; set; }
		public EData Data { get; set; }


		/// <summary>
		/// 2019-03-01 09:53:46 ngocta2
		/// muc dich test, sau nay phai lay tu file config cua app
		/// </summary>
		public string FuPrefixSuffixList = "VN30,VN30F^1M,2M,1Q,2Q|VN100,VN100F^1M,2M,1Q,2Q";
	}
}

/*
{
  "Code": 0,
  "Message": "SUCCESS",
  "Data": {
    "Table": [
      {
        "ASTOCKCODE": "VN30F1909",
        "ACEILPRICE": 958.7,
        "AFLOORPRICE": 833.3,
        "AREFPRICE": 896,
        "ABASICCODE": "VN30",
        "ABASICTYPE": 1,
        "ASTARTDTE": "18/01/2019",
        "AENDDTE": "19/09/2019",
        "APRICEUNIT": 0.1
      },
      {
        "ASTOCKCODE": "VN30F1904",
        "ACEILPRICE": 963.1,
        "AFLOORPRICE": 837.1,
        "AREFPRICE": 900.1,
        "ABASICCODE": "VN30",
        "ABASICTYPE": 1,
        "ASTARTDTE": "22/02/2019",
        "AENDDTE": "18/04/2019",
        "APRICEUNIT": 0.1
      },
      {
        "ASTOCKCODE": "VN30F1903",
        "ACEILPRICE": 963,
        "AFLOORPRICE": 837,
        "AREFPRICE": 900,
        "ABASICCODE": "VN30",
        "ABASICTYPE": 1,
        "ASTARTDTE": "20/07/2018",
        "AENDDTE": "21/03/2019",
        "APRICEUNIT": 0.1
      },
      {
        "ASTOCKCODE": "VN30F1906",
        "ACEILPRICE": 961.8,
        "AFLOORPRICE": 836,
        "AREFPRICE": 898.9,
        "ABASICCODE": "VN30",
        "ABASICTYPE": 1,
        "ASTARTDTE": "19/10/2018",
        "AENDDTE": "20/06/2019",
        "APRICEUNIT": 0.1
      }
    ]
  }
}
 */
