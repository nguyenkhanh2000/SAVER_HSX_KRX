using CommonLib.Interfaces;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace CommonLib.Implementations
{
    /// <summary>
    /// 2019-01-04 14:58:37 ngocta2
    /// class debug tim loi
    /// </summary>
    public class CDebugLogger : CBaseLogger, IDebugLogger
    {
        private const string __TEMPLATE = @"=================
EId     = {0} ({1}) [{2}]
Source  = {3}
Data    = {4}";

        private const string __TEMPLATE_CONTEXT = @"=================
Source  = {0}
EId     = {1} ({2}) [{3}] BeginF
{4}EId     = {5} ({6}) [{7}] EndF <<{8}>>
Finally = {9}";

		private const string __TYPE_FOLDER = __TYPE_FOLDER_DEBUG;//"DEBUG";
//        private bool _flag = false;

        //props
        public bool DebugMode { get { return this._debugFlag; } }


        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="configuration"></param>
        public CDebugLogger(IConfiguration configuration, bool randomFileName = false)
            : base(configuration, __TYPE_FOLDER, randomFileName)
        {
        }

        /// <summary>
        /// 2019-01-03 15:55:18 ngocta2
        /// ghi log error
        /// 2019-01-24 11:21:42 ngocta2
        /// them managedThreadId de phan biet code run giua cac thread khac nhau
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public TExecutionContext LogDebug(string data)
        {            
            string eId = Guid.NewGuid().ToString(); // execution id => chi xuat hien 1 lan execute đó, lần execute sau là id khac

            // chi log khi _flag set = true
            // log lien tuc se gay cham system
            if (this._debugFlag)
                this._logger.Debug(
                    __TEMPLATE,
                    eId,
                    ThreadId,
                    TaskId,
                    GetDeepCaller(),
                    data);

            // return EC object de debug tiep sau do
            return new TExecutionContext()
            {
                Id           = eId,
                Data         = data,
                Timestamp    = Timestamp,
                ThreadId     = ThreadId,
                ThreadIdS    = ThreadIdS,
                TaskId       = TaskId,
                TaskIdS      = TaskIdS,
                FunctionName = FunctionName
            };
        }

        /// <summary>
        /// 2019-01-31 10:26:28 ngocta2
        /// log begin dau function
        /// CHU Y: 1 function body chi duoc co toi da 3 lan ghi vao file log
        /// + 1 lan log debugEnd trong finally block -> log output result + tat ca debug data trong ec.Buffer
        /// + 1 lan log error/errorContext trong catch block -> log error detail
        /// + 1 lan log WriteFileDebugTextAsync tai cuoi try block -> log chi tiet cac value thay doi giua func body, dang luu trong ec.Buffer
        /// 
        /// WriteFileDebugTextAsync giup tach no data hon thanh cac file con tai cac folder khac nhau
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="data"></param>
        public TExecutionContext WriteBufferBegin(string data, bool force = false)
        {
            // tao ec object
            TExecutionContext ec = new TExecutionContext()
            {
                Id           = Guid.NewGuid().ToString(), // execution id => chi xuat hien 1 lan execute đó, lần execute sau là id khac
                Data         = data,
                Timestamp    = Timestamp,
                ThreadId     = ThreadId,
                ThreadIdS    = ThreadIdS,
                TaskId       = TaskId,
                TaskIdS      = TaskIdS,
                FunctionName = FunctionName
            };

            // log buffer
            WriteBufferMid(ec, data, force);

            // return EC object de debug tiep sau do
            return ec;
            
        }

        /// <summary>
        /// 2019-01-25 09:40:55 ngocta2
        /// log debug cuoi function
        /// </summary>
        /// <param name="ec"></param>
        /// <param name="data"></param>
        public void LogDebugContext(TExecutionContext ec, string data)
        {
            // chi log khi _flag set = true
            // log lien tuc se gay cham system
            if (this._debugFlag)
                this._logger.Debug(
                    __TEMPLATE_CONTEXT,
                    GetDeepCaller(),
                    ec.Id,
                    ec.ThreadId,
                    ec.TaskId,
                    ec.Buffer,
                    ec.Id,
                    ec.ThreadId,
                    ec.TaskId,
                    ec.ElapsedMilliseconds,
                    data);
        }

        ///// <summary>
        ///// 2019-01-31 11:00:41 ngocta2
        ///// </summary>
        ///// <param name="ec"></param>
        ///// <param name="data"></param>
        //public void LogDebugBuffer(TExecutionContext ec, string data)
        //{            
        //    // chi write vao buffer khi _flag set = true
        //    // xu ly string nhieu gay cham
        //    if (this._flag)
        //    {
        //        string datetime = DateTime.Now.ToString(EGlobalConfig.DATETIME_LOG);
        //        ec.Buffer.AppendLine($"\t{datetime} => {data}");
        //    }                
        //}

        /// <summary>
        /// 2019-01-03 16:53:30 ngocta2
        /// 2019-01-31 14:30:02 ngocta2
        /// tu monitor se co the call dc vao day de bat che do debug
        /// neu set flag=true thi se ghi log chi tiet rat nhieu 
        /// de xem flow code chay ra sao cac var dang co gia tri gi
        /// viec bat DebugMode se lam cham speed, anh huong performance cua he thong
        /// nen chi bat DebugMode khi can thiet, khi co dau hieu bug ko ro nguyen nhan
        /// </summary>
        /// <param name="flag"></param>
        public void SetDebugMode(bool flag)
        {
            this._debugFlag = flag;
        }

		/// <summary>
		/// nhanh gon, giong nhu write LogEx hoac LogText o cac app cu
		/// </summary>
		/// <param name="data"></param>
		public void LogDebugShort(string data)
		{
			this._logger.Debug(data);
		}

		/// <summary>
		/// 2019-09-26 10:16:48 ngocta2
		/// tuong tu LogSqlSub
		/// </summary>
		/// <param name="executionContext"></param>
		/// <param name="fileName"></param>
		/// <param name="data"></param>
		public void LogDebugSub(TExecutionContext ec, string fileName, string data)
		{
			LogSub(__TEMPLATE_CONTEXT, __TYPE_FOLDER, GetDeepCaller(), ec, fileName, data);
		}
	}
}
