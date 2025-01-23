using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-07 10:28:01 ngocta2
    /// lib common dung chung cho tat ca project
    /// </summary>
    public interface ICommon: IInstance
    {
        string ReadFileNoLock(string fullPath);
        string CheckNullString(string data);
        object CheckNullObject(object data);
        string GetLocalDateTime();// monitor
        string GetLocalIp();// monitor
        int ToInt(object number);
        long ToLong(object number);
        double ToDouble(object number);
		void WriteFile(string fullFilePath, string message, bool append = true);

		Task WriteFileAsync(string filePath, string message, bool append = true);
        /// <summary>
        /// CHU Y: slow speed neu open/close 1 file lien tuc
        /// nen neu write update 1 file lien tuc thi chi duoc call function nay 1 lan trong 1 function body
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="filePath"></param>
        /// <param name="message"></param>
        /// <param name="append"></param>
        /// <returns></returns>
        Task WriteFileDebugTextAsync(TExecutionContext ec, string filePath, string message, bool force = false);
        Task WriteFileDebugTextAsync(TExecutionContext ec, string fullFilePath, string desc, object objectToWrite);
        void SetDebugModeOfDebugger(bool flag);
        string GetTimestamp();
        string SerializeObjectDebug(object objectToSerialize, bool force = false);
        string SerializeObject(object objectToSerialize);
        string AddMinutes(string time, int minuteNumber = 1);
        string AddMinutesEx(string dateTime, int minuteNumber = 1);
        int IndexOfFastest(string ss, string sf);
        bool IsNumericDecimal(string stringToTest);

		long CreateZScore();
		string CreateDateString(string date);
		string CreateTimeString(int time);
		string CreateDateTimeString(string date, string time);
		long CreateZScore4Insert(string symbol, string dateTimeInput);
		long CreateZScore4Update(string dateTimeInput);
		long CreateZScore4BOD(string dateTimeInput);
		long CreateZScore4EOD(string dateTimeInput);

		Task<string> RequestURLAsync(string url);
		DateTime GetPastWorkingDate(int max, List<string> nonworkingDayList);

		long GetCurrentUnixTimestampSeconds();
		int GetUnusedPort(int portStartIndex, int portEndIndex);
		bool IsPortOpen(string ip, int port);

		string Left(string input, int leftCount);
		string Right(string input, int rightCount);
		string Mid(string input, int begin, int count);

        string GetRandomString(int length);
        int GetRandomNumberInRange(int from, int to);

        string GetResultInfo(DataSet dataSet);
        string GetResultInfo(int affectedRowCount);
        long GetStringLength(string inputString);
        string Fix2Json(string fixString);
        DateTime ConvertToUTC(object datetimeString);
        DateTime ConvertToGMT07(object datetimeString);
        string GetMsgType(string rawData);
        string ProcessSqlEscapeChar(string value);
        string FixToDateTimeString(string fixString);
        string FixToTimeString(string fixString);
        string FixToDateString(object fixString);
        string FixToTransDateString(string fixString);
        

        string[] RemoveReturn(string[] rawArray);

        string GetAssemblyVersion(string codeBase);

        string GetIpFull();
        string GetIpShort();
        string GetIpInfo(int all);

        List<string> GetSymbolList(string list);

        T CheckConfig<T>(T config, string fileName, string sectionName, bool dataNull) where T : class;
        T CheckConfig<T>(T config, string fileName, string sectionName) where T : class;
        bool GetMinMaxScore(string tradeDate, ref long fromScore, ref long toScore, string maxScore);
        ERedisKeys.ERealTimeApiService GetRealTimeApiServiceRedisKeys(IConfiguration configuration);

        //void CopyTo(Stream src, Stream dest);
        //byte[] Zip(string str);
        //string Unzip(byte[] bytes);
        string GetProcessOwner(string processName);

        // thieu trong interface
        string ConvertBytes2String(byte[] input);
        bool ByteArrayCompare(byte[] b1, byte[] b2);
    }
}
