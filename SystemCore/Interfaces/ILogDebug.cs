using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Temporaries;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-03 11:09:39 ngocta2
    /// + Log Debug => folder DEBUG
    ///     log tat ca value, de tim bug..... log chi tiet tat ca cho co the log
    /// </summary>
    public interface ILogDebug: ILog
    {

        /// <summary>
        /// prop
        /// </summary>
        bool DebugMode { get; }

        /// <summary>
        /// bat che do log debug  de tim loi
        /// log debug se dat khap noi, dau + giua + cuoi function
        /// </summary>
        /// <param name="flag">true-bat che do ghi log chi tiet tat ca value</param>
        void SetDebugMode(bool flag);

        /// <summary>
        /// ghi chi tiet log debug de tim loi vao file (async)
        /// chi run function nay sau khi SetDebugMode(true)
        /// </summary>
        /// <param name="data"></param>
        /// <returns>
        /// TExecutionContext object
        /// execution id = random string = id danh dau su thuc thi code cua caller tu bat dau func den ket thuc func
        /// context data = detail data trong context do
        /// </returns>
        TExecutionContext LogDebug(string data);

        /// <summary>
        /// 2019-01-25 09:33:47 ngocta2
        /// thuong dung vao cuoi function, sau khi da exec xong het code
        /// truyen them TExecutionContext de tinh toan them
        /// + so luong time da troi qua de exec xong code
        /// </summary>
        /// <param name="data"></param>
        /// <param name="ec"></param>
        /// <returns></returns>
        void LogDebugContext(TExecutionContext ec, string data);

        /// <summary>
        /// 2019-01-31 10:54:56 ngocta2
        /// 2019-01-31 10:26:28 ngocta2
        /// log begin dau function
        /// CHU Y: 1 function body chi duoc co 3 lan ghi log
        /// + 1 lan log debugEnd trong finally block -> log output result 
        /// + 1 lan log error/errorContext trong catch block -> log error detail
        /// + 1 lan log WriteFileDebugTextAsync tai cuoi try block -> log chi tiet cac value thay doi giua func body
        /// </summary>
        /// <param name="data"></param>
        TExecutionContext WriteBufferBegin(string data, bool force = false);

        /// <summary>
        /// 2019-01-31 10:56:32 ngocta2
        /// de han che so lan open/close 1 file qua nhieu gay nghen co chai 
        /// thi tam thoi chi ghi log data vao memory, 
        /// cuoi function body se co code write tat ca data trong mem vao file
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="data"></param>
        void WriteBufferMid(TExecutionContext ec, string data, bool force = false);

		/// <summary>
		/// 2019-08-29 15:00:54 ngocta2
		/// xu ly phai nhanh
		/// + force = true
		/// + template = khong
		/// + return = khong
		/// </summary>
		/// <param name="data"></param>
		void LogDebugShort(string data);

		/// <summary>
		/// 2019-09-26 09:21:35 ngocta2
		/// log data chia nho
		/// D:\WebLog\S6G\TAChartSaverApp\DEBUG\20190926\CalculateChartSymbolAsync-0916.js
		/// D:\WebLog\S6G\TAChartSaverApp\DEBUG\20190926\CalculateChartSymbolAsync-0917.js
		/// D:\WebLog\S6G\TAChartSaverApp\DEBUG\20190926\CalculateChartSymbolAsync-0918.js
		/// </summary>
		/// <param name="executionContext">co the truyen null thi log ra file ko can thong tin ec</param>
		/// <param name="fileName">REDIS-ABT.js</param>
		/// <param name="data">2019-09-20 08:45:09.792 +07:00 [INF] =================...</param>
		void LogDebugSub(TExecutionContext ec, string fileName, string data);
	}
}
