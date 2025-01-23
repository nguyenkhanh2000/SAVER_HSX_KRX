using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Temporaries;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-03 11:09:39 ngocta2
    /// + Log SQL   => folder SQL
    ///     log tat ca sql update vao SQL Server, Oracle, Redis
    /// </summary>
    public interface ILogSql: ILog
    {

        /// <summary>
        /// ghi chi tiet log SQL(SQL+NoSQL) vao file (async)
        /// ghi log sql TRUOC + SAU khi exec sp/script vao db
        /// </summary>
        /// <param name="data"></param>
        void LogSql(string data);


        /// <summary>
        /// log sql co eid de chinh xac hon voi code multi-thread
        /// </summary>
        /// <param name="executionContext"></param>
        /// <param name="data"></param>
        void LogSqlContext(TExecutionContext executionContext, string data);

        /// <summary>
        /// log sql co eid de chinh xac hon voi code multi-thread
        /// </summary>
        /// <param name="executionContext"></param>
        /// <param name="data"></param>
        //void LogSqlContext2(TExecutionContext executionContext, string data);


        /// <summary>
        /// 2019-09-26 09:21:35 ngocta2
        /// log data chia nho
        /// D:\WebLog\S6G\TAChartSaverApp\SQL\20190926\REDIS-ABT.js
        /// D:\WebLog\S6G\TAChartSaverApp\SQL\20190926\REDIS-ACB.js
        /// D:\WebLog\S6G\TAChartSaverApp\SQL\20190926\REDIS-ABI.js
        /// D:\WebLog\S6G\TAChartSaverApp\SQL\20190926\REDIS-VN30F1M.js
        /// </summary>
        /// <param name="executionContext">co the truyen null thi log ra file ko can thong tin ec</param>
        /// <param name="fileName">REDIS-ABT.js</param>
        /// <param name="data">2019-09-20 08:45:09.792 +07:00 [INF] =================...</param>
        void LogSqlSub(TExecutionContext ec, string fileName, string data);
	}
}
