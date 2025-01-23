using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    /// <summary>
    /// 2019-01-15 09:52:36 ngocta2
    /// tat ca config dinh nghia truoc, su dung trong pham vi toan bo he thong Stock6G
    /// </summary>
    static public class EGlobalConfig
    {
        private const string DATETIME_FORMAT_1 = "yyyy-MM-dd HH:mm:ss.fff";
        private const string DATETIME_FORMAT_2 = "yyyyMMddHHmmssfff"; // 5G
        private const string DATETIME_FORMAT_3 = "yyyy-MM-dd";
        private const string __DATETIME_FORMAT_3 = "dd/MM/yyyy";
        private const string DATETIME_FORMAT_4 = "yyyyMMddHHmmss"; // bo qua millisecond
		private const string DATETIME_FORMAT_5 = "yyyyMMddHHmm"; // bo qua second
		private const string DATETIME_FORMAT_6 = "yyyyMMdd"; // bo qua hour : tao score bat dau ngay
		private const string DATETIME_FORMAT_7 = "yyyyMMdd235959999"; // hour fixed : tao score ket thuc ngay
		private const string DATETIME_FORMAT_14 = "yyMMddHHmmss"; // bo qua millisecond
		private const string DATETIME_FORMAT_15 = "yyMMddHHmm"; // bo qua second
		private const string DATETIME_FORMAT_16 = "yyMMdd"; // bo qua hour : tao score bat dau ngay
		private const string DATETIME_FORMAT_17 = "yyMMdd235959999"; // hour fixed : tao score ket thuc ngay
		private const string DATETIME_FORMAT_21 = "HHmmss"; // chi lay gio
		private const string DATETIME_FORMAT_22 = "HHmm"; // chi lay gio
		private const string DATETIME_FORMAT_23 = "HH"; // chi lay gio
		private const string DATETIME_FORMAT_24 = "dd-MMM-yyyy"; // danh cho update date vao db
														

		// select TO_TIMESTAMP('2019-05-17 02:14:26.123', 'YYYY/MM/DD HH24:MI:SS.FF3') FROM DUAL;
		public const string DATETIME_ORACLE = DATETIME_FORMAT_1;

		public const string BLANK_STRING				= "";
		public const string LEADING_ZERO_THREAD_ID      = "0000";
        public const string LEADING_ZERO_TASK_ID        = "000000";
        public const string DATETIME_REDIS_SCORE        = DATETIME_FORMAT_2;
        public const string DATETIME_REDIS_VALUE        = DATETIME_FORMAT_1;
        public const string DATETIME_MONITOR            = DATETIME_FORMAT_1;
        public const string DATETIME_LOG                = DATETIME_FORMAT_1;
        public const string DATETIME_LOG_FILENAME_S     = DATETIME_FORMAT_3;
		//public const string DATETIME_REDIS_SCORE_US     = DATETIME_FORMAT_4;
		//public const string DATETIME_REDIS_SCORE_UM     = DATETIME_FORMAT_5;
		public const string DATETIME_YYYYMMDD			= DATETIME_FORMAT_6;
		//public const string DATETIME_REDIS_SCORE_EOD    = DATETIME_FORMAT_7;
		public const string DATETIME_REDIS_SCORE_YY_US  = DATETIME_FORMAT_14;
		public const string DATETIME_REDIS_SCORE_YY_UM  = DATETIME_FORMAT_15;
		public const string DATETIME_REDIS_SCORE_YY_BOD = DATETIME_FORMAT_16;
		public const string DATETIME_REDIS_SCORE_YY_EOD = DATETIME_FORMAT_17;
		public const string DATETIME_HHMMSS				= DATETIME_FORMAT_21;
		public const string DATETIME_HHMM				= DATETIME_FORMAT_22;
		public const string DATETIME_HH					= DATETIME_FORMAT_23;
		public const string DATETIME_SQL_DATE_ONLY		= DATETIME_FORMAT_24;

		// neu them 1 dai ip moi thi 
		// 1. them vao predefine_ip_1
		// 2. them vao predefine_ip_2
		// 3. build lai SystemCore.dll
		// 4. update lai tat ca cho co lien quan SystemCore.dll
		// predefine_ip_1
		public const string UNKNOWN_IP                = "UnknownIp";
        private const string PREFIX_IP_LAN_FOX        = "172.16.0.";
        private const string PREFIX_IP_LAN_HSX        = "10.26.248.";
        private const string PREFIX_IP_LAN_HNX        = "10.26.100.";
        private const string PREFIX_IP_LAN_FPTS       = "10.26.2.";
        private const string PREFIX_IP_LAN_FPTS_4     = "10.26.4."; // 2018-08-13 16:49:57 Hungpv
        private const string PREFIX_IP_LAN_FPTS_BLAZE = "10.26.5."; // 2018-07-06 08:07:57 ngocta2
        // predefine_ip_2
        public static string[] PREFIX_IP_LIST = new string[]{
            PREFIX_IP_LAN_FOX,
            PREFIX_IP_LAN_HSX,
            PREFIX_IP_LAN_HNX,
            PREFIX_IP_LAN_FPTS,
            PREFIX_IP_LAN_FPTS_4,
            PREFIX_IP_LAN_FPTS_BLAZE
        };

		public const string SUFFIX_REDIS_SCORE_MIN_OF_DAY = "000000000";
		public const string SUFFIX_REDIS_SCORE_MAX_OF_DAY = "235959999";
		static public string DateTimeNow => DateTime.Now.ToString(DATETIME_MONITOR);


		public class Exchange
        {
			public const string __EXCHANGE_HSX = "HSX";
			public const string __EXCHANGE_HNX = "HNX";
		}

		/// <summary>
		/// ID xác định các thị trường
		/// </summary>
		public class MarketID
        {
			//<Sở giao dịch chứng khoán Tp Hồ Chí Minh>
			/// <summary>
			/// STO: Thị trường cổ phiếu (HCM)
			/// </summary>
			public const string __MARKETID_STO = "STO";
			/// <summary>
			/// BDO: Thị trường trái phiếu (HCM)
			/// </summary>
			public const string __MARKETID_BDO = "BDO";
			/// <summary>
			/// RPO: Thị trường Repo (HCM)
			/// </summary>
			public const string __MARKETID_RPO = "RPO";

			//<Sở giao dịch chứng khoán Hà Nội>
			/// <summary>
			/// STX: Thị trường cổ phiếu (HN)
			/// </summary>
			public const string __MARKETID_STX = "STX";
			/// <summary>
			/// BDX: Thị trường trái phiếu chính phủ (HN)
			/// </summary>
			public const string __MARKETID_BDX = "BDX";
			/// <summary>
			/// DVX: Thị trường phái sinh (HN)
			/// </summary>
			public const string __MARKETID_DVX = "DVX";
			/// <summary>
			/// UPX: Thị trường UPCoM (HN)
			/// </summary>
			public const string __MARKETID_UPX = "UPX";
			/// <summary>
			/// HCX: Thị trường trái phiếu doanh nghiệp (HN)
			/// </summary>
			public const string __MARKETID_HCX = "HCX";
		}

		// chi su dung khi run debug cac test trong test project. cu the
		// khi run debug test method thi Path.GetDirectoryName(Assembly.GetEntryAssembly().Location) cho gia tri
		// C:\Users\ngocta2\.nuget\packages\microsoft.testplatform.testhost\15.9.0\lib\netstandard1.5
		// ma dung ra la can path la 
		// D:\TFS\Repos\Stock6G\CommonLib.Tests\bin\Debug\netcoreapp2.2
		// C:\Users\ngocta2\.nuget\packages\microsoft.testplatform.testhost\15.9.0\lib\netstandard1.5 ko co file config json => error
		// public const string __TEST_DEBUG_ROOT_LOG_PATH = @"D:\TFS\Repos\Stock6G";
		public const string __STRING_BLANK_OBJECT = "{}";
		public const string __SERILOG_CONSOLE_TEMPLATE = "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}";
		public const string __DOMAIN_DSACCOUNT = "http://s70.dsaccount.fpts.com.vn";
		public const string __TEMPLATE_OBJECT_PUBLIC = "{\"Text\":\"(Text)\",\"Code\":\"(Code)\"}";
		public const string __TEMPLATE_OBJECT_PUBLIC_CW = "{\"Text\":\"TEXT_CW\",\"Code\":\"CODE_CW\"}";
		public const string __TYPE_MENU_BAR = "bar";
		public const string __TYPE_MENU_ACCOUNT = "account";
		public const string __DATA_NULL = null;

		public const string __STRING_UNKNOWN_IP = "UnknownIp";
		public const string __STRING_BEFORE = "BEFORE: ";
		public const string __STRING_AFTER = "AFTER: ";
		public const string __STRING_BLANK = "";
		public const string __STRING_SPACE = " ";
		public const string __STRING_UNDERSCORE = "_";
		public const string __STRING_TAB = "\t";
		public const string __STRING_RETURN_NEW_LINE = "\r\n";
		public const string __STRING_RETURN_NEW_LINE_NO_RETURN = "\n";
		public const string __STRING_RETURN = "\r";
		public const string __STRING_UNKNOWN = "unknown";
		public const string __STRING_NULL = "null";
		public const string __STRING_0 = "0";
		public const string __STRING_1 = "1";
		public const string __STRING_APPLICATION_JSON = "application/json";
		public const string __STRING_TEXT_PLAIN = "text/plain";
		public const string __STRING_ACCESS_DENIED = "ACCESS_DENIED";
		public const string __STRING_SERVER_VARS_HTTP_USER_AGENT = "HTTP_USER_AGENT";
		public const string __STRING_SERVER_VARS_REMOTE_ADDR = "REMOTE_ADDR";
		public const string __STRING_SERVER_VARS_LOCAL_ADDR = "LOCAL_ADDR";
		public const string __STRING_HEADER_USER_AGENT = "User-Agent";
		public const string __STRING_SUCCESS = "SUCCESS";
		public const string __STRING_FAILED = "FAILED";
		public const string __STRING_GET_SCRIPT = "GetScript";
		public const string __STRING_EXEC = "EXEC";
		public const string __STRING_EQUAL = "=";
		public const string __STRING_EQUAL_GREATER = "=>";
		public const string __STRING_ORACLE_BLOCK_BEGIN = @"
DECLARE
preturncode   NUMBER := -1;
preturnmess   VARCHAR2 (500) := 'INIT';
BEGIN";
		public const string __STRING_ORACLE_BLOCK_END = @"
END;";
		public const string __STRING_METHOD_POST = "POST";
		public const string __STRING_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
		public const string __STRING_COMMA = ",";
		public const string __STRING_ERROR_MSG_NO_CONNECTION = "No connection could be made because the target machine actively refused it."; // unit test error vi webServer ko running
		public const string __FILE_JSON_APPSETTINGS = "appsettings.json";

        public const string __STRING_SQL_BEGIN_TRANSACTION = "BEGIN TRANSACTION";
        public const string __STRING_SQL_COMMIT_TRANSACTION = "COMMIT TRANSACTION";

        public const long __LONG_0 = 0;

		// 2020-07-23 10:52:03 ngocta2 muon tu ben StockGateway.NETCore
		public const long __CODE_SUCCESS = 0;
		public const long __CODE_FAIL = -1;
		public const long __CODE_CHECK_SUCCESS = 1;
		public const long __CODE_CHECK_FAIL = 0;
		public const long __CODE_ERROR_IN_LAYER_DAL = -9998;
		public const long __CODE_ERROR_IN_LAYER_BLL = -9997;
		public const long __CODE_ERROR_IN_LAYER_GUI = -9996;        // controller : tai day thuong return HttpStatusCode.InternalServerError (500)
		public const long __CODE_ERROR_IN_LAYER_UNKOWN = -9995;     // unknown (co the ko bao gio vao duoc day)
		public const long __CODE_ACCESS_DENIED = -123456;       // controller	

		public const int __INIT_NULL_INT = -9999999;
		public const long __INIT_NULL_LONG = -9999999;
		public const double __INIT_NULL_DOUBLE = -9999999;
		public const string __INIT_NULL_STRING = null;

        // Phần này dùng cho HSX 
        public const int __INIT_NULL_INT_HSX = -1;
        public const long __INIT_NULL_LONG_HSX = -1;
        public const double __INIT_NULL_DOUBLE_HSX = -1;
        static public string DateNow => DateTime.Now.ToString(__DATETIME_FORMAT_3);
    }
}
