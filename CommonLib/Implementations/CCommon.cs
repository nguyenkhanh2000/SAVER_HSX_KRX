using CommonLib.Interfaces;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Management;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Interfaces;
using SystemCore.SharedKernel;
using SystemCore.Temporaries;

namespace CommonLib.Implementations
{
	/// <summary>
	/// 2019-01-07 10:31:50 ngocta2
	/// </summary>
	public class CCommon: CInstance, ICommon
    {
        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        static extern int memcmp(byte[] b1, byte[] b2, long count);

        // vars
        private readonly IErrorLogger _errorLogger;
        private readonly IDebugLogger _debugLogger;
        private static object _lockerWriteFile = new object(); // locker, tranh error voi multi-thread code cung write vao 1 file, mat data

        // const 
        public const string __NULL = "null";

        private readonly string[] _base16CharTable = new[]
        {
            "00", "01", "02", "03", "04", "05", "06", "07",
            "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
            "10", "11", "12", "13", "14", "15", "16", "17",
            "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
            "20", "21", "22", "23", "24", "25", "26", "27",
            "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
            "30", "31", "32", "33", "34", "35", "36", "37",
            "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
            "40", "41", "42", "43", "44", "45", "46", "47",
            "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
            "50", "51", "52", "53", "54", "55", "56", "57",
            "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
            "60", "61", "62", "63", "64", "65", "66", "67",
            "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
            "70", "71", "72", "73", "74", "75", "76", "77",
            "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
            "80", "81", "82", "83", "84", "85", "86", "87",
            "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
            "90", "91", "92", "93", "94", "95", "96", "97",
            "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
            "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7",
            "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
            "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7",
            "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
            "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7",
            "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
            "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7",
            "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
            "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7",
            "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
            "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7",
            "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
        };
        private readonly string _flat;
        private const string __STRING_BLANK_OBJECT = "{}";

		private static readonly HttpClient _httpClient = new HttpClient();

		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="errorLogger"></param>
		/// <param name="debugLogger"></param>
		public CCommon(IErrorLogger errorLogger, IDebugLogger debugLogger)
        {
            this._errorLogger = errorLogger;
            this._debugLogger = debugLogger;

            _flat = string.Join("", _base16CharTable);
        }

        // ----------------------
                            

        /// <summary>
        /// https://coderwall.com/p/s-vqdq/fast-byte-array-to-hex-string-conversion-in-c
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public string ConvertBytes2String(byte[] input)
        {
            if (input == null || input.Length < 1)
                return string.Empty;
            int
            x = 0,
            i = 0;
            var sb = new StringBuilder();
            while (i < input.Length)
            {
                x = input[i++] << 1;
                sb.Append(_flat[x]);
                sb.Append(_flat[x + 1]);
            }
            return sb.ToString();
        }

        /// <summary>
        /// 2019-01-07 10:34:12 ngocta2
        /// ReadAllText ko dung duoc khi read file log cua serilog provider => error process khac hold file
        /// phai dung cach nay de read file
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        public string ReadFileNoLock(string fullPath)
        {
            try
            {
                string fileContents;
                using (FileStream fileStream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) // FileShare.ReadWrite
                {
                    using (StreamReader reader = new StreamReader(fileStream))
                    {
                        fileContents = reader.ReadToEnd();
                    }
                }
                return fileContents;
            }
            catch(Exception ex)
            {
                this._errorLogger.LogError(ex);
                return "";
            }
        }

        /// <summary>
        /// 2019-01-10 13:42:13 ngocta2
        /// check null string
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public string CheckNullString(string data)
        {
            if (data == null)
                return __NULL;
            else
                return data;
        }

        /// <summary>
        /// 2019-01-10 13:59:58 ngocta2
        /// check null object
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public object CheckNullObject(object data)
        {
            if (data == null)
                return __NULL;
            else
                return data;
        }

        /// <summary>
        /// 2019-01-15 10:03:16 ngocta2
        /// ke thua S5G func
        /// lay datetime hien tai theo format
        /// </summary>
        /// <returns></returns>
        public string GetLocalDateTime()
        {
            return DateTime.Now.ToString(EGlobalConfig.DATETIME_MONITOR);
        }

        /// <summary>
        /// 2019-01-15 10:03:16 ngocta2
        /// ke thua function cua Stock5G de lay ip local
        /// ------------------------
        /// To get the local IP address 
        ///  lay ip hien tai
        ///  "192.168.2.18,172.16.0.18" => chi lay 172.16.0.18
        /// </summary>
        /// <returns></returns>
        public string GetLocalIp()
        {
            try
            {
                string hostName = Dns.GetHostName();
                System.Net.IPHostEntry ipE = Dns.GetHostEntry(hostName);
                IPAddress[] ipAddresses = ipE.AddressList;
                foreach (IPAddress ip in ipAddresses)
                {
                    string localIP = ip.ToString();
                    foreach (string ipPrefix in EGlobalConfig.PREFIX_IP_LIST)
                        if (localIP.IndexOf(ipPrefix) != -1)
                            return localIP;
                }
                return EGlobalConfig.UNKNOWN_IP;
            }
            catch (Exception ex)
            {
                this._errorLogger.LogError(ex);
                return EGlobalConfig.UNKNOWN_IP;
            }
        }

        /// <summary>
        /// 2019-01-17 15:58:38 ngocta2
        /// chuyen du lieu string thanh int
        /// neu la null string thi thanh 0
        /// </summary>
        /// <param name="number">co the la null string</param>
        /// <returns></returns>
        public int ToInt(object number)
        {
            if (number == null)
                return 0;
            else
            {
                if (int.TryParse(number.ToString(), out int n))
                    return Convert.ToInt32(number);
                else
                    return 0;
            }
        }

        /// <summary>
        /// 2019-01-16 13:42:39 ngocta2
        /// chuyen du lieu string thanh long
        /// neu la null string thi thanh 0
        /// </summary>
        /// <param name="number">co the la null string</param>
        /// <returns></returns>
        public long ToLong(object number)
        {
            if (number == null)
                return 0;
            else
            {
                if (long.TryParse(number.ToString(), out long n))
                    return Convert.ToInt64(number);
                else
                    return 0;
            }                
        }

        /// <summary>
        /// 2019-01-16 13:42:39 ngocta2
        /// chuyen string thanh double
        /// neu null thi thanh 0
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        public double ToDouble(object number)
        {
            if (number == null)
                return 0;
            else
            {
                if (double.TryParse(number.ToString(), out double n))
                    return Convert.ToDouble(number);
                else
                    return 0;
            }
        }

		/// <summary>
		/// 2019-03-13 15:14:02 ngocta2
		/// </summary>
		/// <param name="fullFilePath"></param>
		/// <param name="message"></param>
		/// <param name="append"></param>
		public void WriteFile(string fullFilePath, string message, bool append = true)
		{
			// kiem tra folder trong full path, neu ko co thi tao folder
			FileInfo fileInfo = new FileInfo(fullFilePath);
			if (!fileInfo.Directory.Exists)
				fileInfo.Directory.Create();


			using (FileStream stream = new FileStream(fullFilePath, append ? FileMode.Append : FileMode.Create, FileAccess.Write, FileShare.None, 4096, true))
			using (StreamWriter sw = new StreamWriter(stream))
			{
				sw.WriteLine(message);
			}
		}

		/// <summary>
		/// https://stackoverflow.com/questions/11774827/writing-to-a-file-asynchronously
		/// uu diem
		/// + co the tao nhieu file khac nhau
		/// + async mode giup tranh bi nghen voi code multithread
		/// </summary>
		/// <param name="fullFilePath"></param>
		/// <param name="message"></param>
		/// <param name="append"></param>
		/// <returns></returns>
		public async Task WriteFileAsync(string fullFilePath, string message, bool append = true)
        {
            // kiem tra folder trong full path, neu ko co thi tao folder
            //FileInfo fileInfo = new FileInfo(fullFilePath);
            //if (!fileInfo.Directory.Exists)
            //    fileInfo.Directory.Create();


            // 2020-04-17 15:58:03 ngocta2
            // System.IO.IOException : Cannot create 'D:\WebLog\S6G\TAChartSaverAppLib.Tests' because a file or directory with the same name already exists.
            // https://stackoverflow.com/questions/4036804/automatically-create-directories-from-long-paths
            string dir = System.IO.Path.GetDirectoryName((fullFilePath));
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);



            using (FileStream stream = new FileStream(fullFilePath, append ? FileMode.Append : FileMode.Create, FileAccess.Write, FileShare.None, 4096, true))
            using (StreamWriter sw = new StreamWriter(stream))
            {
                await sw.WriteLineAsync(message);
            }
        }

        /// <summary>
        /// 2019-01-17 15:31:49 ngocta2
        /// tuong tu WriteFileAsync, chi khac la co check them flag DebugMode
        /// CHU Y: slow speed neu open/close 1 file lien tuc
        /// nen neu write update 1 file lien tuc thi chi duoc call function nay 1 lan trong 1 function body
        /// VD: ko log = 13s; log 1 lan = 15s; log 8 lan ~ 120s
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="fullFilePath"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task WriteFileDebugTextAsync(TExecutionContext ec, string fullFilePath, string message,  bool force = false)
        {
            // chi write file neu flag debug = true
            if (this._debugLogger.DebugMode || force)
            {
                if (ec == null)
                    ec = new TExecutionContext() { Id = "noId", Data = "noData" };
                if(ec.Buffer.Length==0)
                    await WriteFileAsync(fullFilePath, DateTime.Now.ToString(EGlobalConfig.DATETIME_MONITOR) + " | " + ec.Id + " | " + message);
                else
                    await WriteFileAsync(fullFilePath, DateTime.Now.ToString(EGlobalConfig.DATETIME_MONITOR) + " | " + ec.Id + " ============== " + Environment.NewLine + "Buffer --- " + Environment.NewLine + ec.Buffer.ToString() + "Finally --- " + message);
            }                
        }

        /// <summary>
        /// Message     = Collection was modified; enumeration operation may not execute.
        /// CTAChartSaverAppLib.cs:line 917 => Newtonsoft.Json.JsonConvert.SerializeObject(this._jsonADic[symbol])
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="fullFilePath"></param>
        /// <param name="desc"></param>        
        /// <param name="objectToWrite"></param>
        /// <returns></returns>
        public async Task WriteFileDebugTextAsync(TExecutionContext ec, string fullFilePath, string desc, object objectToWrite)
        {
            // chi write file neu flag debug = true
            if (this._debugLogger.DebugMode)
            {
                if (ec == null)
                    ec = new TExecutionContext() { Id = "noId", Data = "noData" };
                await WriteFileAsync(fullFilePath, DateTime.Now.ToString(EGlobalConfig.DATETIME_MONITOR) + " | " + ec.Id + " | " + desc + " | " + Newtonsoft.Json.JsonConvert.SerializeObject(objectToWrite));
            }
        }

        /// <summary>
        /// 2019-01-31 09:54:34 ngocta2
        /// CHU Y:
        // 0. su dung ZeroFormatter de Serialize/Deserialize nhanh hon json.net tu 10-5000 lan
        // 1. Serialize cua ZeroFormatter cho ra output la bytes[] chu ko phai string nhu json.net
        // 2. write bytes[] vao file binary chi nen ghi 1 row, neu can ghi nhieu row thi define 1 big obj chua list 
        // 3. ko the them ghi chu nao khac vao file binary ma phai ghi chu tren filename, datetime thi xem modified date time cua file
        // 4. binary data phu hop cho cac cho can ghi log nhieu hoac co the luu ET data
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="fullFilePath"></param>
        /// <param name="objectToWrite"></param>
        /// <returns></returns>
        public async Task WriteFileDebugBinaryAsync(TExecutionContext ec, string fullFilePath, object objectToWrite)
        {
            // serialize objectToWrite => byte[]
            // write byte[] to file

            // chi write file neu flag debug = true
            if (this._debugLogger.DebugMode)
            {
                if (ec == null)
                    ec = new TExecutionContext() { Id = "noId", Data = "noData" };
                await WriteFileAsync(fullFilePath, Newtonsoft.Json.JsonConvert.SerializeObject(objectToWrite));
            }
        }

        /// <summary>
        /// 2019-01-29 17:15:06 ngocta2
        /// can 1 function SerializeObject nhanh nhat co the => Json.net qua cham neu phai log/debug nhieu 
        /// 
        /// Message     = Collection was modified; enumeration operation may not execute.
        /// https://medium.com/@neuecc/zeroformatter-fastest-c-serializer-and-infinitely-fast-deserializer-for-net-88e752803fe9
        /// https://github.com/neuecc/ZeroFormatter
        /// https://jacksondunstan.com/articles/3318
        /// https://designingefficientsoftware.wordpress.com/2011/03/03/efficient-file-io-from-csharp/
        /// </summary>
        /// <param name="objectToSerialize"></param>
        /// <returns></returns>
        public string SerializeObjectDebug(object objectToSerialize, bool force=false)
        {          
            try
            {
                // chi write file neu flag debug = true
                if (this._debugLogger.DebugMode || force)
                {
                    return Newtonsoft.Json.JsonConvert.SerializeObject(objectToSerialize); // SLOW SPEED
                }                    
                else
                    return __STRING_BLANK_OBJECT;
            }
            catch 
            {
                return __STRING_BLANK_OBJECT; // failed
            }
        }

        /// <summary>
        /// log sql
        /// </summary>
        /// <param name="objectToSerialize"></param>
        /// <returns></returns>
        public string SerializeObject(object objectToSerialize)
        {
            try
            {
                return Newtonsoft.Json.JsonConvert.SerializeObject(objectToSerialize); // SLOW SPEED
            }
            catch
            {
                return __STRING_BLANK_OBJECT; // failed
            }
        }


        //[ZeroFormattable]
        //public class MyClass
        //{
        //    // Index is key of serialization
        //    [Index(0)]
        //    public virtual int Age { get; set; }

        //    [Index(1)]
        //    public virtual string FirstName { get; set; }

        //    [Index(2)]
        //    public virtual string LastName { get; set; }

        //    // When mark IgnoreFormatAttribute, out of the serialization target
        //    [IgnoreFormat]
        //    public string FullName { get { return FirstName + LastName; } }

        //    [Index(3)]
        //    public virtual IList<int> List { get; set; }
        //}

        /// <summary>
        /// 2019-01-30 11:14:22 ngocta2
        /// ZeroFormatterSerializer nhanh hon JSON.net
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objectToSerialize"></param>
        /// <returns></returns>
        //public string SerializeObjectZero<T>(T objectToSerialize)
        //{
        //    try
        //    {

        //        var mc = new MyClass
        //        {
        //            Age = 99,
        //            FirstName = "hoge",
        //            LastName = "huga",
        //            List = new List<int> { 1, 10, 100 }
        //        };

        //        var bytes = ZeroFormatterSerializer.Serialize(mc);
        //        var mc2 = ZeroFormatterSerializer.Deserialize<MyClass>(bytes);
        //        // ZeroFormatter.DynamicObjectSegments.MyClass
        //        Console.WriteLine(mc2.GetType().FullName);

        //        //EJsonADetail eJsonADetail = new EJsonADetail() { ServerTime = "10:48" };
        //        //byte[] bytes = ZeroFormatterSerializer.Serialize(eJsonADetail);
        //        //return System.Text.Encoding.UTF8.GetString(bytes);

        //        // chi write file neu flag debug = true
        //        if (this._debugLogger.DebugMode)
        //        {
        //            //byte[] bytes = ZeroFormatterSerializer.Serialize(objectToSerialize);
        //            //return  System.Text.Encoding.UTF8.GetString(bytes); 
        //            return "";
        //        }
        //        else
        //            return "{}";
        //    }
        //    catch 
        //    {
        //        return "{}"; // failed
        //    }
        //}

        /// <summary>
        /// 2019-01-17 15:44:32 ngocta2
        /// ko the sua vao SystemCore.ICommon vi ref nguoc tu CORE vao INF
        /// nen phai tao them function nay
        /// </summary>
        /// <param name="flag"></param>
        public void SetDebugModeOfDebugger(bool flag)
        {
            this._debugLogger.SetDebugMode(flag);
        }


        /// <summary>
        /// 20190111172524128
        /// </summary>
        /// <returns></returns>
        public string GetTimestamp()
        {
            return DateTime.Now.ToString(EGlobalConfig.DATETIME_REDIS_SCORE);
        }


        /// <summary>
        /// https://stackoverflow.com/questions/43289/comparing-two-byte-arrays-in-net
        /// fastest way
        /// </summary>
        /// <param name="b1"></param>
        /// <param name="b2"></param>
        /// <returns></returns>
        public bool ByteArrayCompare(byte[] b1, byte[] b2)
        {
            try
            {
                // Validate buffers are the same length.
                // This also ensures that the count does not exceed the length of either buffer.  
                return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
            }
            catch
            {
                return false; // failed
            }
        }


        /// <summary>
        /// 2019-02-01 13:52:47 ngocta2
        /// </summary>
        /// <param name="time">09:59</param>
        /// <returns>10:00</returns>
        public string AddMinutes(string time, int minuteNumber = 1)
        {
            try
            {
                // 1. input
                int h = Convert.ToInt32(time.Substring(0, 2));
                int m = Convert.ToInt32(time.Substring(3, 2));

                TimeSpan timeSpan = new TimeSpan(h, m, 0);
                TimeSpan timeSpan2 = new TimeSpan(h, m, 0);

                timeSpan2 = timeSpan.Add(new TimeSpan(0, minuteNumber, 0));

                return timeSpan2.ToString().Substring(0, 5);
            }
            catch
            {
                return time; // failed
            }
        }

        /// <summary>
        /// 2019-02-01 14:04:32 ngocta2
        /// 
        /// </summary>
        /// <param name="dateTime">"2019-01-24 10:03"</param>
        /// <param name="minuteNumber"></param>
        /// <returns>"2019-01-24 10:04"</returns>
        public string AddMinutesEx(string dateTime, int minuteNumber = 1)
        {
            try
            {
                string date = dateTime.Substring(0, 10);
                string time = dateTime.Substring(11, 5);
                string newTime = AddMinutes(time);
                string newDateTime = $"{date} {newTime}";

                return newDateTime; // success
            }
            catch
            {
                return dateTime; // failed
            }
        }


        /// <summary>
        /// http://cc.davelozinski.com/c-sharp/fastest-way-to-check-if-a-string-occurs-within-a-string
        /// </summary>
        /// <param name="ss">//the strings to search</param>
        /// <param name="sf">//the chars/strings to look for. We use both because we're testing some string methods too.</param>
        /// <returns>
        /// 1 - found sf in ss
        /// 0 - not found sf in ss
        /// </returns>
        public int IndexOfFastest(string ss, string sf)
        {
            return((ss.Length - ss.Replace(sf, String.Empty).Length) / sf.Length > 0 ? 1 : 0);         
        }


        /// <summary>
        /// D:\Project\Stock5G\BaseLib\CBase.cs
        /// </summary>
        /// <param name="stringToTest"></param>
        /// <returns></returns>
        public bool IsNumericDecimal(string stringToTest)
        {
            decimal result;
            return decimal.TryParse(stringToTest, out result);
        }

		/// <summary>
		/// phai luu static de so sanh xem co trung voi so cu ko
		/// </summary>
		static long lastZScore = 0;
		/// <summary>
		/// 
		/// cach cu "yyyyMMddHHmmssfff" toan bi trung, ngay ca trong 1 ms cung van trung 4,5 row
		/// trung score gay ra insert data vao redis that bai => result = true la success nhung check redis van ko co data
		/// them 3 so sau cung la tran so kieu int64
		/// them 2 so 0 o sau cung 
		/// them 2 so thi luu redis thanh 2.0190226130143498e+18 => KHONG DUOC
		/// ===================================
		/// cach nay du xai trong 100 nam
		/// tinh dateDiff giua now va 1/1/2019 00:00:00 tinh theo milliseconds * 1000
		/// neu trung nhau thi +1 cho den khi khac nhau
		/// </summary>
		/// <returns></returns>
		public long CreateZScore()
		{
			long zScore = 0;
			DateTime begin = new DateTime(2019, 1, 1, 0, 0, 0, 0);
			DateTime now = DateTime.Now;
			long diffInMs = Convert.ToInt64((now - begin).TotalMilliseconds);
			zScore = diffInMs * 1000;
			while (zScore <= lastZScore)
				zScore++; // +1 cho den khi khac so score truoc
			lastZScore = zScore;
			return zScore;
		}

		/// <summary>
		/// luu last zScore tung symbol
		/// </summary>
		static ConcurrentDictionary<string, long> _lastZScoreDic = new ConcurrentDictionary<string, long>();
		/// <summary>
		/// 2019-03-06 09:52:32 ngocta2
		/// chart data dang co 2 loai chinh
		/// + data tung phut: score can ngan gon, de dang update row khi chi can so sanh da co row voi score cu thi delete / add row co score do
		/// + data chi tiet: score can dai de ko bi trung nhau, chi add row chu ko update
		/// ca 2 loai data tren (neu la HSX) thi can xoa sach luc start app, de tranh bi add row double
		/// 
		/// CreateZScore4Insert => tao score luon phai distinct, ko duoc trung nhau, chi dung cho insert row
		/// CreateZScore4Update => tao score ko phai distinct, duoc trung nhau, se dung cho update row => (delete row + insert row where score = score dang co)
		/// 
		/// phai su dung dic vi score rieng cua moi symbol la khac nhau
		/// 
		/// CHU Y: moi lan goi la +1 vao value , ko goi thua`
		/// CHU Y: phai dung zkey, ko the dung symbol vi sai logic, 1 symbol co nhieu zKey khac nhau
		/// </summary>
		/// <param name="zKey"> "INTRADAY:S6G__FPT" / "INTRADAYDETAIL:S6G__FPT" </param>
		/// <param name="dateTimeInput"> "2019-03-05 09:15:36.123" / "2019-03-05 09:15:36" / "2019-03-05 09:15" </param>
		/// <returns></returns>
		public long CreateZScore4Insert(string zKey, string dateTimeInput)
		{
			long STEP = 1;
			long zScore = 0, lastZScore = 0;
			string dateTimeString = dateTimeInput;
			// "2019-03-05 09:15:36" => #2019-03-05 09:15:36#
			DateTime dateTime = DateTime.Parse(dateTimeString);
			//  #2019-03-05 09:15:36# => "20190305091536"
			dateTimeString = dateTime.ToString(EGlobalConfig.DATETIME_REDIS_SCORE_YY_US); //yyyyMMddHHmmss
																					   // "20190305091536" => 20190305091536
			zScore = Convert.ToInt64(dateTimeString);
			// 20190305091536 => 20190305091536000 
			zScore = zScore * 1000;
			//if (symbol == "ACB") WriteFile(@"D:\ACB.js", $"(A) zScore={zScore}");
			// dic chua co thi add value 0, co roi thi lay ra
			if (_lastZScoreDic.ContainsKey(zKey))
				lastZScore = _lastZScoreDic[zKey];
			else
				_lastZScoreDic.TryAdd(zKey, zScore);
			//if (symbol == "ACB") WriteFile(@"D:\ACB.js", $"(B) lastZScore={lastZScore}; _lastZScoreDic[{symbol}]={_lastZScoreDic[symbol]}");
			while (zScore <= lastZScore)
				zScore += STEP; // +1 cho den khi khac so score truoc
			//if (symbol == "ACB") WriteFile(@"D:\ACB.js", $"(C) zScore={zScore}");
			// update dic
			_lastZScoreDic.TryUpdate(zKey, zScore, lastZScore);
			//if (symbol == "ACB") WriteFile(@"D:\ACB.js", $"(D) lastZScore={zScore}; _lastZScoreDic[{symbol}]={_lastZScoreDic[symbol]}");
			return zScore;
		}

		/// <summary>
		/// 2019-03-06 14:04:21 ngocta2
		/// dau vao chi co den phut, ko co giay
		/// ko can check trung nhau
		/// </summary>
		/// <param name="dateTimeInput"> "2019-03-05 09:15:36.123" / "2019-03-05 09:15:36" / "2019-03-05 09:15" </param>
		/// <returns></returns>
		public long CreateZScore4Update(string dateTimeInput)
		{
			return CreateZScoreByTemplate(dateTimeInput, EGlobalConfig.DATETIME_REDIS_SCORE_YY_UM, 100000);

			//long zScore = 0;
			//string dateTimeString = dateTimeInput;
			//// "2019-03-05 09:15:36" => #2019-03-05 09:15:36#
			//DateTime dateTime = DateTime.Parse(dateTimeString);
			////  #2019-03-05 09:15:36# => "20190305091536"
			//dateTimeString = dateTime.ToString(EGlobalConfig.DATETIME_REDIS_SCORE_UM); //yyyyMMddHHmm
			//// "201903050915" => 201903050915
			//zScore = Convert.ToInt64(dateTimeString);
			//// 201903050915 => 20190305091500000
			//zScore = zScore * 100000;
			//return zScore;
		}

		/// <summary>
		/// 2019-03-06 14:13:22 ngocta2
		/// tao score bat dau ngay
		/// ket hop score EOD va score BOD de xoa data trong ngay khi start app, tranh bi dup data
		/// </summary>
		/// <param name="dateTimeInput"> "2019-03-05 09:15:36.123" / "2019-03-05 09:15:36" / "2019-03-05 09:15" / "2019-03-05" / 20190305 </param>
		/// <returns></returns>
		public long CreateZScore4BOD(string dateTimeInput)
		{
			return CreateZScoreByTemplate(dateTimeInput, EGlobalConfig.DATETIME_REDIS_SCORE_YY_BOD, 1000000000);
		}

		/// <summary>
		/// 2019-03-06 14:13:22 ngocta2
		/// tao score ket thuc ngay
		/// ket hop score EOD va score BOD de xoa data trong ngay khi start app, tranh bi dup data
		/// </summary>
		/// <param name="dateTimeInput"> "2019-03-05 09:15:36.123" / "2019-03-05 09:15:36" / "2019-03-05 09:15" / "2019-03-05" / 20190305 </param>
		/// <returns></returns>
		public long CreateZScore4EOD(string dateTimeInput)
		{
			return CreateZScoreByTemplate(dateTimeInput, EGlobalConfig.DATETIME_REDIS_SCORE_YY_EOD, 1);
		}

		public long CreateZScoreByTemplate(string dateTimeInput, string dateTimeFormat, long multiply)
		{
			long zScore = 0;
			string dateTimeString = dateTimeInput;
			// xu ly date ko co - . VD: 20190305
			if (dateTimeString.Length == 8)
				dateTimeString = dateTimeString.Insert(4, "-").Insert(7, "-");
			// "2019-03-05 09:15:36" => #2019-03-05 09:15:36#
			DateTime dateTime = DateTime.Parse(dateTimeString);
			//  #2019-03-05 09:15:36# => "20190305091536"
			dateTimeString = dateTime.ToString(dateTimeFormat);
			// "20190305" => 20190305
			zScore = Convert.ToInt64(dateTimeString);
			// 20190305 => 20190305000000000
			zScore = zScore * multiply;
			// return
			return zScore;
		}

		/// <summary>
		/// 2019-03-01 08:49:10 ngocta2
		/// https://stackoverflow.com/questions/4015324/how-to-make-http-post-web-request
		/// https://serverfault.com/questions/800338/why-is-iis-allowing-only-3-connections-at-the-time
		/// </summary>
		/// <param name="url"></param>
		/// <returns></returns>
		public async Task<string> RequestURLAsync(string url)
		{

			// log buffer begin
			TExecutionContext ec = _debugLogger.WriteBufferBegin($"url={url}");

			try
			{
				string responseString = await _httpClient.GetStringAsync(url);
				return responseString;
			}
			catch (Exception ex)
			{
				// log error
				_errorLogger.LogErrorContext(ex, ec); // Response status code does not indicate success: 503 (Service Unavailable).
                return "";
			}
			finally
			{
				// log debug
				_debugLogger.LogDebugContext(ec, $"end");
			}
		}

		/// <summary>
		/// 2019-03-08 08:53:49 ngocta2
		/// </summary>
		/// <param name="date">20190308</param>
		/// <returns>2019-03-08</returns>
		public string CreateDateString(string date)
		{
			try
			{
				string d = date.Insert(4, "-").Insert(7, "-");
				DateTime dt = DateTime.Parse(d);
				return d;
			}
			catch (Exception ex)
			{
				_errorLogger.LogError(ex);
				return "";
			}
		}

		/// <summary>
		/// 2019-03-08 10:02:45 ngocta2
		/// 2019-02-20 15:26:59 ngocta2
		/// 91932 => "091932"
		/// 101932 => "101932"
		/// </summary>
		/// <param name="time"></param>
		/// <returns></returns>
		public string CreateTimeString(int time)
		{
			string timeString = "";
			if (time < 100000)
				timeString = $"0{time}";
			else
				timeString = time.ToString();

			string d = timeString.Insert(2, ":").Insert(5, ":");

			return d;
		}

		/// <summary>
		/// 2019-03-08 08:53:49 ngocta2
		/// </summary>
		/// <param name="date"></param>
		/// <param name="time"></param>
		/// <returns></returns>
		public string CreateDateTimeString(string date, string time)
		{
			return $"{CreateDateString(date)} {time}";
		}


		/// <summary>
		/// lay 1 ngay co giao dich trong qua khu
		/// 
		/// 
		/// max=3; 
		/// list=["2019-02-16","2019-02-17","2019-02-23","2019-02-24","2019-03-02","2019-03-03","2019-03-09","2019-03-10"]
		/// 
		/// hom nay thu 2		ngay 11/3 (dang gio giao dich) >>>>>> NOW = 3/11/2019 1:57:48 PM
		/// hom qua chu nhat	ngay 10/3 (ngay nghi)
		/// hom kia thu 7		ngay 09/3 (ngay nghi)
		///			thu 6		ngay 08/3 (ngay giao dich)
		///			thu 5		ngay 07/3 (ngay giao dich)
		///			thu 4		ngay 06/3 (ngay giao dich)
		///			thu 3		ngay 05/3 (ngay giao dich)
		///	
		/// vay thi ket qua la pastWorkingDate=3/6/2019 1:57:48 PM; 
		/// 
		/// </summary>
		/// <param name="max">so ngay giao dich trong qua khu gan day nhat</param>
		/// <param name="nonworkingDayList">["2019-02-16","2019-02-17","2019-02-23","2019-02-24","2019-03-02","2019-03-03","2019-03-09","2019-03-10"]</param>
		/// <returns></returns>
		public DateTime GetPastWorkingDate(int max, List<string> nonworkingDayList)
		{
			DateTime pastWokingDate = DateTime.Now.AddDays(-max);
			for (int i = nonworkingDayList.Count - 1; i > 0; i--)
			{
				string dateString = nonworkingDayList[i];
				DateTime dt = Convert.ToDateTime(dateString);
				if (pastWokingDate < dt && dt < DateTime.Now) // neu ngay nghi co nam trong khoang ngay ket qua va ngay hien tai thi -1
					pastWokingDate = pastWokingDate.AddDays(-1);
			}
			return pastWokingDate;
		}


		/// <summary>
		/// 2019-07-08 14:02:18 ngocta2
		/// original : D:\Source\Repos\Stock6G\TAChartApiService\Utils\TimeUnixServices.cs
		/// luu data trong redis theo t la UnixTime
		/// {"Time":"2019-04-01 17:05:00.853","Data":{"t":"1554094800","o":17.0,"h":17.45,"l":17.0,"c":17.3,"v":5152510}}
		/// </summary>
		/// <returns></returns>
		public long GetCurrentUnixTimestampSeconds()
		{
			DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
			return (long)(DateTime.UtcNow - UnixEpoch).TotalSeconds;
		}

		/// <summary>
		/// 2019-08-05 10:32:11 ngocta2
		/// lay randon dang available (chua co service/app/web nao su dung port do)
		/// </summary>
		/// <param name="portStartIndex"></param>
		/// <param name="portEndIndex"></param>
		public int GetUnusedPort( int portStartIndex, int portEndIndex )
		{
			try
			{
				IPGlobalProperties properties = IPGlobalProperties.GetIPGlobalProperties();
				IPEndPoint[] tcpEndPoints = properties.GetActiveTcpListeners();

				List<int> usedPorts = tcpEndPoints.Select(p => p.Port).ToList<int>();
				int unusedPort = 0;

				for (int port = portStartIndex; port < portEndIndex; port++)
				{
					if (!usedPorts.Contains(port))
					{
						unusedPort = port;
						break;
					}
				}

				return unusedPort;
			}
			catch 
			{
				return 0;
			}			
		}

		/// <summary>
		/// 2019-08-16 15:55:51 ngocta2
		/// kiem tra port da used chua
		/// </summary>
		/// <param name="ip"></param>
		/// <param name="port"></param>
		/// <returns></returns>
		public bool IsPortOpen(string ip, int port)
		{
			TimeSpan timeout = TimeSpan.FromMilliseconds(100);
			try
			{
				using (var client = new TcpClient())
				{
					var result = client.BeginConnect(ip, port, null, null);
					var success = result.AsyncWaitHandle.WaitOne(timeout);
					if (!success)
					{
						//Console.WriteLine($"{GetDateTimeNow()} - IsPortOpen=false(1)");
						return false;
					}

					client.EndConnect(result);
				}

			}
			catch
			{
				//Console.WriteLine($"{GetDateTimeNow()} - IsPortOpen=false(2) - ex.Message={ex.Message};");
				return false;
			}

			//Console.WriteLine($"{GetDateTimeNow()} - IsPortOpen=true; _createSubDone={_createSubDone}");
			return true;
		}

		//https://www.arclab.com/en/kb/csharp/string-operations-left-mid-right-comparision-csharp-mfc.html
		public string Left(string input, int leftCount)
		{
			if (string.IsNullOrEmpty(input))
				return "";
			if (input.Length < leftCount)
				return input;
			else
				return input.Substring(0, leftCount);
		}

		//https://www.arclab.com/en/kb/csharp/string-operations-left-mid-right-comparision-csharp-mfc.html
		public string Right(string input, int rightCount)
		{
			if (string.IsNullOrEmpty(input))
				return "";
			if (input.Length < rightCount)
				return input;
			else
				return input.Substring(input.Length - rightCount, rightCount);
		}

		//https://www.arclab.com/en/kb/csharp/string-operations-left-mid-right-comparision-csharp-mfc.html
		public string Mid(string input, int begin, int count)
		{
			if (string.IsNullOrEmpty(input))
				return "";
			if (input.Length < begin)
				return "";
			if (begin <= 0)
				begin = 1;

			return input.Substring(begin - 1, count);
        }

		/// <summary>
		/// 2019-09-09 10:39:15 ngocta2
		/// tuong tu WriteFile nhung la static method de co the su dung ma ko can init instance class truoc 
        //<2020-08-28 13:43:32.819> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=3; 
        //<2020-08-28 13:43:34.698> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=4; 
        //<2020-08-28 13:43:36.548> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=5; 
        //<2020-08-28 13:43:38.426> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=6; 
        //<2020-08-28 13:43:40.325> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=7; 
        //<2020-08-28 13:43:42.291> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=8; 
        //<2020-08-28 13:43:44.242> - CreateSubscriber; subSocket.ReceiveFrameString DONE; _messageCount=9; 
		/// </summary>
		/// <param name="fullFilePath"></param>
		/// <param name="message"></param>
		static public bool WriteFileStatic(string fullFilePath, string message)
		{
			try
			{
                // lock allows only one thread to execute the code at the same time (aug/3/2020 4:40 PM)
                // SPEED VERY SLOW => HIGH SPEED CODE phai bo call func nay
                lock (_lockerWriteFile)
				{
                    // kiem tra folder trong full path, neu ko co thi tao folder
                    // System.IO.DirectoryNotFoundException: 'Could not find a part of the path 'M:\WebLog\S6G\CommonLib.Tests'.'
                    FileInfo fileInfo = new FileInfo(fullFilePath);
                    if (!fileInfo.Directory.Exists)
                        fileInfo.Directory.Create();

                    using (FileStream stream = new FileStream(fullFilePath, FileMode.Append, FileAccess.Write, FileShare.None, 4096, true))
                    using (StreamWriter sw = new StreamWriter(stream))
                    {
                        sw.WriteLine(message);
                    }
                }

				return true;
			}
			catch// (Exception ex)
			{
				return false;
			}
		}

		/// <summary>
		/// get folder hien tai cua Assembly
		/// </summary>
		/// <returns></returns>
		static public string GetCurrDir()
		{
			try
			{				
				return Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
			}
			catch// (Exception ex)
			{
				return "";
			}
		}

        /// <summary>
        /// 2019-11-20 13:38:54 ngocta2
        /// https://stackoverflow.com/questions/1344221/how-can-i-generate-random-alphanumeric-strings
        //Guid.NewGuid().ToString()
        //"9f22d404-9e9d-4d18-89d9-ee4b6ccbf8b3"
        //Guid.NewGuid().ToString("n")
        //"16b09d154b284e6194e5083c23a83aa0"
        //Guid.NewGuid().ToString("n").ToUpper()
        //"DF3F91B1A27E486AB544CFA7B312FC80"
        //Guid.NewGuid().ToString("n").ToUpper().Substring(0,4)
        //"A022"
        /// </summary>
        /// <param name="length">max 32</param>
        /// <returns></returns>
        public string GetRandomString(int length)
        {
            return Guid.NewGuid().ToString("n").ToUpper().Substring(0, length);
        }

        /// <summary>
        /// 2019-11-20 14:41:53 ngocta2
        /// https://stackoverflow.com/questions/2706500/how-do-i-generate-a-random-int-number
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public int GetRandomNumberInRange(int from, int to)
        {
            Random rnd = new Random();
            return rnd.Next(from, to + 1);  // creates a number between 1 and 12

        }

        /// <summary>
        /// 2019-11-26 10:20:55 ngocta2
        /// lay thong tin ve dataset
        /// </summary>
        /// <param name="dataSet"></param>
        /// <returns></returns>
        public string GetResultInfo(DataSet dataSet)
        {
            int tableCount = 0;
            string infoString;
            if (dataSet == null)
                infoString = "dataSet=null";
            else
            {
                infoString = $"dataSet.Tables.Count={dataSet.Tables.Count}; ";
                foreach (DataTable dataTable in dataSet.Tables)
                    infoString += $"dataTable[{tableCount++}].Rows.Count={dataTable.Rows.Count}; ";
            }

            return infoString;
        }

        /// <summary>
        /// 2019-11-26 10:20:55 ngocta2
        /// lay thong tin ve cac row da bi anh huong bi sql
        /// </summary>
        /// <param name="affectedRowCount"></param>
        /// <returns></returns>
        public string GetResultInfo(int affectedRowCount)
        {
            return $"affectedRowCount={affectedRowCount}";
        }

        /// <summary>
        /// 2020-07-08 15:57:31 ngocta2
        /// tra ve so byte , do dai cua string (UNICODE la 2 bytes, ANSI = 1 byte)
        //Encoding.UTF8.GetByteCount("chars")
        //5
        //Encoding.UTF8.GetByteCount("charsô")
        //7
        /// </summary>
        /// <param name="inputString"></param>
        /// <returns></returns>
        public long GetStringLength(string inputString)
        {
            if (inputString == null)
                return 0;
            else
                return Encoding.UTF8.GetByteCount(inputString);
        }

        /// <summary>
        /// 2020-07-22 15:14:42 ngocta2
        /// convert FIX string sang JSON string
        /// yeu cau speed phai nhanh nhat co the
        /// input   : FIX string
        /// output  : JSON string
        /// </summary>
        /// <param name="fixString">8=FIX.4.49=51935=d49=VNMGW56=9999934=152=20190517 09:14:26.12830001=STO20004=G4911=3851207=HO55=VN000000KMR230624=KMR30628=172930629=MIRAE Joint Stock Company30630=MIRAE Joint Stock Company20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000083225=231=1.0223=0.015=VND20020=13660000001149=999999999.01148=-999999999.0202=0.0965=N30631=1.01193=236=0.020013=4930.020014=0.020015=0.020016=0.0140=4930.020027=330642=30511=30301=2017010130614=220018=0030625=0.030635=NRM30636=SNE30637=NRM10=056</param>
        /// <returns>{"8":"FIX.4.4","9":"519","35":"d","49":"VNMGW","56":"99999","34":"1","52":"20190517 09:14:26.128","30001":"STO","20004":"G4","911":"3851","207":"HO","55":"VN000000KMR2","30624":"KMR","30628":"1729","30629":"MIRAE Joint Stock Company","30630":"MIRAE Joint Stock Company","20009":"S1STOST","20003":"STO","30604":"ST","201":"","1194":"","541":"","106":"ID00000083","225":"","231":"1.0","223":"0.0","15":"VND","20020":"1366000000","1149":"999999999.0","1148":"-999999999.0","202":"0.0","965":"N","30631":"1.0","1193":"","236":"0.0","20013":"4930.0","20014":"0.0","20015":"0.0","20016":"0.0","140":"4930.0","20027":"3","30642":"","30511":"","30301":"20170101","30614":"2","20018":"00","30625":"0.0","30635":"NRM","30636":"SNE","30637":"NRM","10":"056"}</returns>
		public string Fix2Json(string fixString)
		{
            StringBuilder sb = new StringBuilder(fixString);
            sb.Length--;
            sb.Replace("", "\",\"");
            sb.Replace("=", "\":\"");
            sb.Append("\"}");
            sb.Insert(0, "{\"");
            return sb.ToString();
        }


        /// <summary>
        /// 2020-07-23 16:44:41 ngocta2
        /// </summary>
        /// <param name="rawString">20190517 09:14:26.128</param>
        /// <returns></returns>
        public DateTime ConvertToUTC(object raw)
        {
            string rawString = raw.ToString();
            DateTime localDateTime, univDateTime;            
            string datetimeString = rawString.Insert(4, "-").Insert(7, "-");
            localDateTime = DateTime.Parse(datetimeString);
            univDateTime = localDateTime.ToUniversalTime();
            return univDateTime;
        }

        /// <summary>
        /// 2020-08-06 15:05:14 ngocta2
        /// </summary>
        /// <param name="raw"></param>
        /// <returns></returns>
        public DateTime ConvertToGMT07(object raw)
        {
            string rawString = raw.ToString();
            DateTime localDateTime;
            string datetimeString = rawString.Insert(4, "-").Insert(7, "-");
            localDateTime = DateTime.Parse(datetimeString);
            return localDateTime;
        }

        /// <summary>
        /// 2020-07-30 16:45:01 ngocta2
        /// xu ly raw data cua MDDS
        /// </summary>
        /// <param name="rawData">8=FIX.4.49=51935=d49=VNMGW56=9999934=152=20190517 09:14:26.12830001=STO20004=G4911=3851207=HO55=VN000000KMR230624=KMR30628=172930629=MIRAE Joint Stock Company30630=MIRAE Joint Stock Company20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000083225=231=1.0223=0.015=VND20020=13660000001149=999999999.01148=-999999999.0202=0.0965=N30631=1.01193=236=0.020013=4930.020014=0.020015=0.020016=0.0140=4930.020027=330642=30511=30301=2017010130614=220018=0030625=0.030635=NRM30636=SNE30637=NRM10=056</param>
        /// <returns>d</returns>
        public string GetMsgType(string rawData)
		{
            string msgType= Regex.Match(rawData, "35=(.*?)", RegexOptions.Multiline).Groups[1].Value;
            return msgType;
        }
        
        
        /// <summary>
        /// 2020-08-04 14:47:24 ngocta2
        /// xu ly cac ky tu dac biet lam error qua trinh exec sql script
        /// </summary>
        /// <param name="value">Foreigners' Current Room Test Stock #2</param>
        /// <returns>Foreigners'' Current Room Test Stock #2</returns>
		public string ProcessSqlEscapeChar(string value)
		{
            string processed = value;
            processed = processed.Replace("'", "''");
            return processed;
        }

        /// <summary>
        /// 2020-08-07 11:07:20 ngocta2
        /// </summary>
        /// <param name="fixString">20190517 09:14:26.128</param>
        /// <returns>2019-05-17 09:14:26.128</returns>
        public string FixToDateTimeString(string fixString)
        {            
            DateTime localDateTime;
            string datetimeString = fixString.Insert(4, "-").Insert(7, "-");
            localDateTime = DateTime.Parse(datetimeString);
            string processed = localDateTime.ToString(EGlobalConfig.DATETIME_ORACLE);
            return processed;
        }

        /// <summary>
        /// 2020-08-10 15:27:24 ngocta2
        /// </summary>
        /// <param name="fixString">085500028</param>
        /// <returns>08:55:00.028</returns>
        public string FixToTimeString(string fixString)
        {
            string inputTime = fixString.Insert(2, ":").Insert(5, ":").Insert(8, ".");
            DateTime utc0Time = DateTime.ParseExact(inputTime, "HH:mm:ss.fff", null);

            // Chuyển đổi sang múi giờ UTC+7
            DateTime utc7Time = utc0Time.AddHours(7);
            string datetimeString = utc7Time.ToString("HH:mm:ss.fff");
            return datetimeString;
        }

        /// <summary>
        /// 2020-09-14 hungtq2
        /// </summary>
        /// <param name="fixString">20190217</param>
        /// <returns>19-feb-2019</returns>

        public string FixToTransDateString(string fixString)
        {
            DateTime localDateTime;
            localDateTime = DateTime.Parse(fixString);
            string processed = localDateTime.ToString(EGlobalConfig.DATETIME_SQL_DATE_ONLY);
            return processed;
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// </summary>
        /// <param name="fixString">20190217</param>
        /// <returns>19-feb-2019</returns>

        public string FixToDateString(object fixString)
        {
            DateTime localDateTime;
            string rawString = fixString.ToString();
            string datetimeString = rawString.Insert(4, "-").Insert(7, "-");
            localDateTime = DateTime.Parse(datetimeString);
            string processed = localDateTime.ToString(EGlobalConfig.DATETIME_SQL_DATE_ONLY); // "dd-MMM-yyyy"
            return processed;
        }


        /// <summary>
        /// 2020-08-31 16:02:09 ngocta2
        /// xoa ky tu xuong dong
        /// </summary>
        /// <param name="rawArray"></param>
        /// <returns></returns>
        public string[] RemoveReturn(string[] rawArray)
        {
            for (int i = 0; i < rawArray.Length; i++)

                rawArray[i] = Regex.Replace(
                    Regex.Replace(rawArray[i], EGlobalConfig.__STRING_RETURN, EGlobalConfig.__STRING_BLANK)
                    , EGlobalConfig.__STRING_RETURN_NEW_LINE_NO_RETURN, EGlobalConfig.__STRING_BLANK);
            return rawArray;
        }

        /// <summary>
        /// 2021-03-03 15:18:50 ngocta2
        /// lay version cua assemmbly , phai pass vao func
        /// string codeBase      = Assembly.GetExecutingAssembly().CodeBase;
        /// </summary>
        /// <param name="codeBase">file:///D:/Source/Repos/Stock6G/PriceService/bin/Debug/netcoreapp3.1/PriceService.dll</param>
        /// <returns></returns>
        public string GetAssemblyVersion(string codeBase)
        {            
            UriBuilder uri       = new UriBuilder(codeBase);
            string path          = Uri.UnescapeDataString(uri.Path);
            FileVersionInfo info = FileVersionInfo.GetVersionInfo(path);
            string buildDate     = System.IO.File.GetLastWriteTime(path).ToString(EGlobalConfig.DATETIME_MONITOR);
            EVersion version     = new EVersion() { Version = info.FileVersion, Note = info.Comments, BuildDate = buildDate };
            string json          = SerializeObject(version);
            return json;
        }

        /// <summary>
        /// 2021-03-04 13:14:30 ngocta2
        /// day du thong tin ve ip
        /// tat ca ip tren tat ca adapter
        /// </summary>
        /// <returns>[{"Id":"{ACA17E1C-2CFF-48AD-8EBE-FF265317BAC3}","Name":"10.26.2.33","Description":"Realtek PCIe GBE Family Controller","NetworkInterfaceType":6,"OperationalStatus":1,"Speed":100000000,"IsReceiveOnly":false,"SupportsMulticast":true,"DnsSuffix":"fpts.com.vn","IsDnsEnabled":false,"IsDynamicDnsEnabled":true,"UnicastIPAddresses":["fe80::7538:dad9:974c:f22f%12","10.26.2.33"],"MulticastAddresses":["ff01::1%12","ff02::1%12","ff02::c%12","ff02::fb%12","ff02::1:3%12","ff02::1:ff4c:f22f%12","224.0.0.1","224.0.0.251","224.0.0.252","239.255.255.250"],"AnycastAddresses":[],"GatewayAddresses":["10.26.2.1"],"DnsAddresses":["10.26.248.14","10.26.248.16"],"DhcpServerAddresses":["10.26.248.14"]},{"Id":"{E0EC89D8-2A3A-11EB-BA6E-806E6F6E6963}","Name":"Loopback Pseudo-Interface 1","Description":"Software Loopback Interface 1","NetworkInterfaceType":24,"OperationalStatus":1,"Speed":1073741824,"IsReceiveOnly":false,"SupportsMulticast":true,"DnsSuffix":"","IsDnsEnabled":false,"IsDynamicDnsEnabled":true,"UnicastIPAddresses":["::1","127.0.0.1"],"MulticastAddresses":["ff02::c%1","239.255.255.250"],"AnycastAddresses":[],"GatewayAddresses":[],"DnsAddresses":["fec0:0:0:ffff::1%1","fec0:0:0:ffff::2%1","fec0:0:0:ffff::3%1"],"DhcpServerAddresses":[]},{"Id":"{43D5BD57-8952-4FAF-A455-1004F5554ED5}","Name":"vEthernet (WSL)","Description":"Hyper-V Virtual Ethernet Adapter","NetworkInterfaceType":6,"OperationalStatus":1,"Speed":10000000000,"IsReceiveOnly":false,"SupportsMulticast":true,"DnsSuffix":"","IsDnsEnabled":false,"IsDynamicDnsEnabled":false,"UnicastIPAddresses":["fe80::1fb:b758:f3d3:1c4a%27","172.20.16.1"],"MulticastAddresses":["ff01::1%27","ff02::1%27","ff02::c%27","ff02::fb%27","ff02::1:ffd3:1c4a%27","224.0.0.1","224.0.0.251","239.255.255.250"],"AnycastAddresses":[],"GatewayAddresses":[],"DnsAddresses":["fec0:0:0:ffff::1%1","fec0:0:0:ffff::2%1","fec0:0:0:ffff::3%1"],"DhcpServerAddresses":[]}]</returns>        
        public string GetIpFull()
        {
            List<EIP> ips = new List<EIP>();
            NetworkInterface[] adapters = NetworkInterface.GetAllNetworkInterfaces();

            foreach (NetworkInterface adap in adapters)
            {
                IPInterfaceProperties prop       = adap.GetIPProperties();
                List<string> unicastIPAddresses  = new List<string>();
                List<string> multicastAddresses  = new List<string>();
                List<string> anycastAddresses    = new List<string>();
                List<string> gatewayAddresses    = new List<string>();
                List<string> dnsAddresses        = new List<string>();
                List<string> dhcpServerAddresses = new List<string>();

                // unicast + multicast + anycast
                foreach (UnicastIPAddressInformation e in prop.UnicastAddresses)
                    unicastIPAddresses.Add(e.Address.ToString());
                foreach (MulticastIPAddressInformation e in prop.MulticastAddresses)
                    multicastAddresses.Add(e.Address.ToString());
                foreach (IPAddressInformation e in prop.AnycastAddresses)
                    anycastAddresses.Add(e.Address.ToString());

                // gateway + dns + dhcp
                foreach (GatewayIPAddressInformation e in prop.GatewayAddresses)
                    gatewayAddresses.Add(e.Address.ToString());
                foreach (IPAddress e in prop.DnsAddresses)
                    dnsAddresses.Add(e.ToString());
                foreach (IPAddress e in prop.DhcpServerAddresses)
                    dhcpServerAddresses.Add(e.ToString());


                ips.Add(new EIP()
                {
                    Id                   = adap.Id,
                    Name                 = adap.Name,
                    Description          = adap.Description,
                    NetworkInterfaceType = (int)adap.NetworkInterfaceType,
                    OperationalStatus    = (int)adap.OperationalStatus,
                    Speed                = adap.Speed,
                    IsReceiveOnly        = adap.IsReceiveOnly,
                    SupportsMulticast    = adap.SupportsMulticast,
                    DnsSuffix            = prop.DnsSuffix,
                    IsDnsEnabled         = prop.IsDnsEnabled,
                    IsDynamicDnsEnabled  = prop.IsDynamicDnsEnabled,
                    UnicastIPAddresses   = unicastIPAddresses,
                    MulticastAddresses   = multicastAddresses,
                    AnycastAddresses     = anycastAddresses,
                    GatewayAddresses     = gatewayAddresses,
                    DnsAddresses         = dnsAddresses,
                    DhcpServerAddresses  = dhcpServerAddresses
                });
            }

            return SerializeObject(ips);
        }


        /// <summary>
        /// 2021-03-04 13:21:28 ngocta2
        /// lay thong tin ip ngan gon
        /// </summary>
        /// <returns>["10.26.2.33","172.20.16.1"]</returns>
        public string GetIpShort()
        {
            NetworkInterface[] adapters = NetworkInterface.GetAllNetworkInterfaces();
            List<string> ips = new List<string>();

            foreach (NetworkInterface adap in adapters)
            {
                IPInterfaceProperties prop = adap.GetIPProperties();

                // bo qua type loopback
                if (adap.NetworkInterfaceType != NetworkInterfaceType.Loopback)
                {
                    // chi lay IPv4 cua unicast ( chu yeu la cua type Ethernet, Ethernet co the la physical/virtual/docker )
                    foreach (UnicastIPAddressInformation e in prop.UnicastAddresses)
                    {
                        if (!e.Address.IsIPv6LinkLocal)
                        {
                            ips.Add(e.Address.ToString());
                        }
                    }
                }
            }

            return this.SerializeObject(ips);
        }

        /// <summary>
        /// lay thong tin ip cua machine
        /// </summary>
        /// <param name="all">1</param>
        /// <returns></returns>
        public string GetIpInfo(int all)
        {
            if (all == 1)
                return GetIpFull();
            else
                return GetIpShort();
        }

        /// <summary>
        /// lay list symbol tu 1 param
        /// </summary>
        /// <param name="list">
        /// FPT
        /// FPT,SSI
        /// VN30
        /// </param>
        /// <returns>FPT,SSI,VNM</returns>
        public List<string> GetSymbolList(string list)
        {
            List<string> symbolList = new List<string>();

            // truong hop 1 ma (FPT) hoac 1 list ma (FPT,SSI)
            string[] arr = list.Split(',');
            foreach (string str in arr)
                symbolList.Add(str);

            // truong hop 1 ten ro (VN30)
            // hsxBasketList
            // hnxBasketList
            // neu list = ten ro thi return list cac ma cua ro do
            // sap lam ............

            return symbolList;
        }

        /// <summary>
        /// 2021-03-15 10:39:49 ngocta2
        /// kiem tra config da lay dung chua, neu chua thi lay lai config 
        /// binh thuong run F5 debug project webApi thi read file config ok (RedisKeys.json)
        /// nhung khi run unit test tro vao controller cua project webApi thi ko read dc file RedisKeys.json
        /// nen can code xu ly neu ko read dc file config thi phai read lai file config
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="config"></param>
        /// <param name="fileName"></param>
        /// <param name="sectionName"></param>
        /// <param name="dataNull"></param>
        /// <returns></returns>
        public T CheckConfig<T>(T config, string fileName, string sectionName, bool dataNull) where T: class 
        {
            if (dataNull)
            {
                ConfigurationBuilder builder = new ConfigurationBuilder();
                builder.SetBasePath(Directory.GetCurrentDirectory());  // errors here
                builder.AddJsonFile(fileName, optional: false, reloadOnChange: false);
                IConfiguration configuration = builder.Build();
                configuration.GetSection(sectionName).Bind(config);                
            }

            return config;
        }

        public T CheckConfig<T>(T config, string fileName, string sectionName) where T : class
        {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.SetBasePath(Directory.GetCurrentDirectory());  // errors here
            builder.AddJsonFile(fileName, optional: false, reloadOnChange: false);
            IConfiguration configuration = builder.Build();
            configuration.GetSection(sectionName).Bind(config);            

            return config;
        }

        /// <summary>
        /// lay dung block HsxApiService or HnxApiService trong d:\source\repos\stock6g\HSXApiService\RedisKeys.json 
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public ERedisKeys.ERealTimeApiService GetRealTimeApiServiceRedisKeys(IConfiguration configuration)
        {
            ERedisKeys.ERealTimeApiService redisKeysAS = new ERedisKeys.ERealTimeApiService();
            string subSection = configuration.GetSection($"{ERedisConfig.__SECTION_REDISCONFIG}:{ERedisConfig.__SECTION_KEYSECTIONSUB}").Value;
            redisKeysAS = new ERedisKeys.ERealTimeApiService();
            redisKeysAS = this.CheckConfig<ERedisKeys.ERealTimeApiService>(redisKeysAS, ERedisKeys.__FILENAME, ERedisKeys.__SECTION_REDISKEYS + ":" + subSection);
            return redisKeysAS;
        }


        /// <summary>
        /// 2021-03-26 10:52:49 hungtq2
        /// 2 trường hợp
        /// + trong giờ giao dịch thì lấy từ maxScore,vd: js gọi 5s 1 lần 
        /// + nếu vào ngày nghỉ(lễ,t7,cn) thì lấy fromScore , toScore của ngày giao dịch gần nhất
        /// </summary>
        /// <param name="tradeDate">210325: là ngày giao dịch gần đây nhất (có thể là hôm nay), nguồn data lấy từ redis key MARKETSTATUS:S6G_HSX</param>
        /// <param name="fromScore">210325000000000</param>
        /// <param name="toScore">210325235959999</param>
        /// <param name="maxScore">210325135959999</param>
        /// <returns></returns>
        public bool GetMinMaxScore(string tradeDate, ref long fromScore, ref long toScore, string maxScore)
        {
            string fullScoreBegin, fullScoreEnd;
            long maxScorEnd;
            fullScoreBegin = tradeDate + EGlobalConfig.SUFFIX_REDIS_SCORE_MIN_OF_DAY;
            fullScoreEnd   = tradeDate + EGlobalConfig.SUFFIX_REDIS_SCORE_MAX_OF_DAY;
            fromScore      = Convert.ToInt64(fullScoreBegin);
            toScore        = Convert.ToInt64(fullScoreEnd);
            
            if (maxScore  != null)
            {
                maxScorEnd = Int64.Parse(maxScore);
                if (maxScorEnd < fromScore)
                {
                    fromScore = 0;
                    toScore = 0;
                }
                else
                {
                    fromScore = maxScorEnd;
                }
                return true;
            }
            return false;
        }

        //https://stackoverflow.com/questions/7343465/compression-decompression-string-with-c-sharp
        public static void CopyTo(Stream src, Stream dest)
        {
            byte[] bytes = new byte[4096];

            int cnt;

            while ((cnt = src.Read(bytes, 0, bytes.Length)) != 0)
            {
                dest.Write(bytes, 0, cnt);
            }
        }

        //https://stackoverflow.com/questions/7343465/compression-decompression-string-with-c-sharp
        public static byte[] Zip(string str)
        {
            var bytes = Encoding.UTF8.GetBytes(str);

            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(mso, CompressionMode.Compress))
                {
                    //msi.CopyTo(gs);
                    CopyTo(msi, gs);
                }

                return mso.ToArray();
            }
        }

        //https://stackoverflow.com/questions/7343465/compression-decompression-string-with-c-sharp
        public static string Unzip(byte[] bytes)
        {
            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                {
                    //gs.CopyTo(mso);
                    CopyTo(gs, mso);
                }

                return Encoding.UTF8.GetString(mso.ToArray());
            }
        }

        /// <summary>
        /// lay ten username run app
        /// </summary>
        /// <param name="processName">notepad.exe</param>
        /// <returns>FIT-NGOCTA2\ngocta2</returns>
        public string GetProcessOwner(string processName)
        {
            string query = "Select * from Win32_Process Where Name = \"" + processName + "\"";
            ManagementObjectSearcher searcher = new ManagementObjectSearcher(query);
            ManagementObjectCollection processList = searcher.Get();

            foreach (ManagementObject obj in processList)
            {
                string[] argList = new string[] { string.Empty, string.Empty };
                int returnVal = Convert.ToInt32(obj.InvokeMethod("GetOwner", argList));
                if (returnVal == 0)
                {
                    // return DOMAIN\user
                    string owner = argList[1] + "\\" + argList[0];
                    return owner;
                }
            }

            return "NO OWNER";
        }

        public static string GetCaller(int level = 2)
        {
            var m = new StackTrace().GetFrame(level).GetMethod();

            if (m.DeclaringType == null) return ""; //9:33 AM 6/18/2014 Exception Details: System.NullReferenceException: Object reference not set to an instance of an object.

            // .Name is the name only, .FullName includes the namespace
            var className = m.DeclaringType.FullName;

            //the method/function name you are looking for.
            var methodName = m.Name;

            //returns a composite of the namespace, class and method name.
            return className + "->" + methodName;
        }

        /// <summary>
        /// ke thua tu 5G
        /// 
        /// 3=>0 = Microsoft.Samples.AspNetRouteIntegration.Service->HelloWorld=>BaseLib.CSQL->ExecuteSP=>BaseLib.CBase->GetDeepCaller=>BaseLib.CBase->GetCaller=>
        /// 3=>2 = Microsoft.Samples.AspNetRouteIntegration.Service->HelloWorld=>BaseLib.CSQL->ExecuteSP=>
        /// </summary>
        /// <returns></returns>
        public static string GetCallStack()
        {
            string strCallerName = "";
            for (int i = 3; i >= 2; i--)
                strCallerName += GetCaller(i) + "=>";

            //returns a composite of the namespace, class and method name.
            return strCallerName;
        }

        /// <summary>
        /// 2019-01-03 16:16:35 ngocta2
        /// ke thua tu 5G: xac dinh vi tri function dang run
        /// </summary>
        /// <returns></returns>
        public static string GetDeepCaller()
        {
            string strCallerName = "";
            for (int i = 3; i >= 3; i--)
                strCallerName += GetCaller(i);//

            //returns a composite of the namespace, class and method name.
            return strCallerName;
        }


    }
}
