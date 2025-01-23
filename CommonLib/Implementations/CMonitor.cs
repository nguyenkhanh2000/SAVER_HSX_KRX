using CommonLib.Interfaces;
using Microsoft.Extensions.Configuration;
using MonitorCore.Enums;
using MonitorCore.Interfaces;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Entities;
using SystemCore.Interfaces;

namespace CommonLib.Implementations
{
    /// <summary>
    /// 2019-01-15 09:12:18 ngocta2
    /// monitor toan bo he thong Stock6G
    /// </summary>
    

    public class CMonitor : IMonitor
    {
        // vars
        private readonly IErrorLogger   _errorLogger;
        private readonly IDebugLogger   _debugLogger;
        private readonly ICommon        _common;
        private readonly IConfiguration _configuration;

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="errorLogger"></param>
        /// <param name="debugLogger"></param>
        public CMonitor(CRedis_New RC)
        {
            this.m_RC = RC;
        }
        public CMonitor(IErrorLogger errorLogger, IDebugLogger debugLogger, ICommon common, IConfiguration configuration)
        {
            this._errorLogger   = errorLogger;
            this._debugLogger   = debugLogger;
            this._common        = common;
            this._configuration = configuration;

          

        }

        public CRedis_New m_RC;
        static object m_objLocker = new object();
        public string m_strChannelMonitor = "S5G_MONITOR";
        private const string TEMPLATE_JSON_TYPE_STATUS_FEEDER = "[{\"Time\":\"(DateTimeMonitor)\",\"Type\":\"2\",\"Data\":[{\"RowID\":\"(RowID)\",\"Info\":[[2,\"(StartedTime)\"],[6,\"(ActiveTime)\"],[7,\"(RowCount)\"],[8,\"(DurationFeeder)\"]]}]}]";
        public Dictionary<string, string> m_dicStartedTime = new Dictionary<string, string>();
        private const string FORMAT_DATETIME = "yyyy-MM-dd HH:mm:ss.fff";

        public static string[] ARRAY_MONITOR_APP = {
                                                 "HSXS5.0",             //0
                                                 "HSXF5.0Q",            //1
                                                 "HSXF5.0I",            //2
                                                 "HSXF5.0PT",           //3
                                                 "HNXS5.0",             //4
                                                 "HNXF5.0",             //5
                                                 "HNXF4.5",             //6
                                                 "IIS_Root",            //7
                                                 "IIS_VD_HSX",          //8
                                                 "IIS_VD_HNX",          //9
                                                 "IIS_VD_CHART",        //10
                                                 "WEB_MONITOR",         //11
                                                 "HSXF5.0",             //12
                                                 "HNXF5.0Pro",          //13
                                                 "HSXF5.0Pro",          //14
                                                 "IIS_VD_Pro_HNX",      //15
                                                 "IIS_VD_Pro_HSX",      //16
                                                 "HNXS5.0DB",           //17
                                                 "QRHO5.0",	            //18        2017-12-02 15:47:56 ngocta2
                                                 "QRHA5.0",	            //19
                                                 "QRUP5.0",	            //20
                                                 "HNX_LOGON",	        //21//2018-11-01 17:39:11 hungpv
												 "Fu2_CO_GetPrice",	    //22//2019-10-15 14:04:38 ngocta2
                                                 "HSXF5.5Q",	        //23//2020-10-27 12:32:46 ngocta2
                                                 "HSXF5.5I",	        //24//2020-10-27 12:32:46 ngocta2
                                                 "HSXF5.5",             //25
                                                 "HSXS5_0RD"
                                             };

        public enum MONITOR_APP
        {
            HSX_Saver5G = 0,
            HSX_Feeder5G_Q = 1,
            HSX_Feeder5G_I = 2,
            HSX_Feeder5G_PT = 3,
            HNX_Saver5G = 4,
            HNX_Feeder5G = 5,
            HNX_Feeder45G = 6,
            IIS_Root = 7,
            IIS_VD_HSX = 8,
            IIS_VD_HNX = 9,
            IIS_VD_CHART = 10,
            Web_Monitor = 11,
            HSX_Feeder5G = 12,
            HNXPro_Feeder5G = 13,
            HSXPro_Feeder5G = 14,
            IIS_VD_HNX_Pro = 15,
            IIS_VD_HSX_Pro = 16,
            HNX_Saver5G_DB = 17, // monitor chuc nang insert db68 cua HNX_SAVER (insert data InfoGate)
            QuoteReader_HO5G = 18,
            QuoteReader_HA5G = 19,
            QuoteReader_UP5G = 20,
            HNX_Logon_Saver5G = 21,//2018-11-01 16:59:50 hungpv them vao redis logon canh bao doi mat khau
            Fu2_CO_GetPrice = 22,//2019-10-15 14:04:59 ngocta2 giam sat tinh trang pub data gia vao rabbit queue cua lenh dieu kien
            HSX_FeederEx_Q = 23,//2020-10-23 09:06:58 ngocta2
            HSX_FeederEx_I = 24,//2020-10-23 09:06:58 ngocta2
            HSX_Feeder55G = 25,//2020-11-04 08:35:48 ngocta2
            HSX_Saver5G_Redis = 26,
        }

        /// <summary>
        /// 2019-01-15 09:16:42 ngocta2
        /// tu enum lay ra app name
        /// </summary>
        /// <param name="appList"></param>
        /// <returns></returns>
        public string GetAppName(AppList appList)
        {
            switch (appList)
            {
                case AppList.HSXSimulatorApp        : return "HSXSimulatorApp";
                case AppList.HNXSimulatorApp        : return "HNXSimulatorApp";
                case AppList.HSXGatewayApp          : return "HSXGatewayApp";
                case AppList.HNXGatewayApp          : return "HNXGatewayApp";
                case AppList.HSXFeederApp           : return "HSXFeederApp";
                case AppList.HNXFeederApp           : return "HNXFeederApp";
                case AppList.HSXSaverApp            : return "HSXSaverApp";
                case AppList.HNXSaverApp            : return "HNXSaverApp";
                case AppList.DataService            : return "DataService";
                case AppList.SysAgentService        : return "SysAgentService";
                case AppList.MonitorWeb             : return "MonitorWeb";
                case AppList.RootApiService         : return "RootApiService";
                case AppList.ChartApiService        : return "ChartApiService";
                case AppList.HSXApiService          : return "HSXApiService";
                case AppList.HNXApiService          : return "HNXApiService";
                case AppList.PriceBoardWeb          : return "PriceBoardWeb";
                case AppList.LivePriceWeb           : return "LivePriceWeb";
                case AppList.MarketWatchEzTradeWeb  : return "MarketWatchEzTradeWeb";
                case AppList.MarketWatchEzFuturesWeb: return "MarketWatchEzFuturesWeb";
                case AppList.TAChartSaverApp        : return "TAChartSaverApp";
                case AppList.TAChartApiService      : return "TAChartApiService";
                case AppList.TAChartWeb             : return "TAChartWeb";
                default                             : return "";
            }
        }

        public bool SendStatusToMonitor(string strActiveTime, string strIP, MONITOR_APP MA, int intRowCount, long lngDuration)
        {
            try
            {
                string strJsonB = TEMPLATE_JSON_TYPE_STATUS_FEEDER;// "[{\"Time\":\"(DateTimeMonitor)\",\"Type\":\"2\",\"Data\":[{\"RowID\":\"(RowID)\",\"Info\":[[2,\"(StartedTime)\"],[6,\"(ActiveTime)\"],[7,\"(RowCount)\"],[8,\"(DurationFeeder)\"]]}]}]";
                string strAppName = /*GetAppName(appList);*/ARRAY_MONITOR_APP[(int)MA];
                string strRowID = strAppName;
                string strKey = strRowID;
                string strOut = "";

                // xu ly dic
                if (!m_dicStartedTime.TryGetValue(strKey, out strOut))
                    m_dicStartedTime.Add(strKey, strActiveTime);// add new key

                // tao json
                strJsonB = strJsonB.Replace("(DateTimeMonitor)", DateTime.Now.ToString(FORMAT_DATETIME));
                strJsonB = strJsonB.Replace("(RowID)", strRowID);
                strJsonB = strJsonB.Replace("(StartedTime)", m_dicStartedTime[strKey]);
                strJsonB = strJsonB.Replace("(ActiveTime)", strActiveTime);
                strJsonB = strJsonB.Replace("(RowCount)", intRowCount.ToString());
                strJsonB = strJsonB.Replace("(DurationFeeder)", lngDuration.ToString());

                lock (m_objLocker) // lock multi-thread => Message requested was not found in the queue specified
                {
                    var taskRC1 = Task.Run(() =>
                    {
                        try
                        {
                            if (this.m_RC.RC_1 != null)
                            {
                                // pub vao channel monitor
                                this.m_RC.RC_1.Publish(this.m_strChannelMonitor, strJsonB);
                            }
                        }
                        catch (Exception ex)
                        {
                            this._errorLogger.LogError(ex);
                        }
                    });

                    var taskRC2 = Task.Run(() =>
                    {
                        try
                        {
                            if (this.m_RC.RC_2 != null)
                            {
                                this.m_RC.RC_2.Publish(this.m_strChannelMonitor, strJsonB);
                            }
                        }
                        catch (Exception ex)
                        {
                            this._errorLogger.LogError(ex);
                        }
                    });
                    Task.WhenAll(taskRC1, taskRC2).Wait();
                }

                return true;
            }
            catch (Exception ex)
            {
                throw ex;
                //CLog.LogError(CBase.GetDeepCaller(), CBase.GetDetailError(ex));
                return false;
            }
        }
    /*    public bool SendStatusToMonitor_2(string strActiveTime, string strIP, MONITOR_APP MA, int intRowCount, long lngDuration,string host1,int port1, string host2, int port2)
        {
            try
            {
                string strJsonB = TEMPLATE_JSON_TYPE_STATUS_FEEDER;// "[{\"Time\":\"(DateTimeMonitor)\",\"Type\":\"2\",\"Data\":[{\"RowID\":\"(RowID)\",\"Info\":[[2,\"(StartedTime)\"],[6,\"(ActiveTime)\"],[7,\"(RowCount)\"],[8,\"(DurationFeeder)\"]]}]}]";
                string strAppName = ARRAY_MONITOR_APP[(int)MA];
                string strRowID = strAppName;
                string strKey = strRowID;
                string strOut = "";

                // xu ly dic
                if (!m_dicStartedTime.TryGetValue(strKey, out strOut))
                    m_dicStartedTime.Add(strKey, strActiveTime);// add new key

                // tao json
                strJsonB = strJsonB.Replace("(DateTimeMonitor)", DateTime.Now.ToString(FORMAT_DATETIME));
                strJsonB = strJsonB.Replace("(RowID)", strRowID);
                strJsonB = strJsonB.Replace("(StartedTime)", m_dicStartedTime[strKey]);
                strJsonB = strJsonB.Replace("(ActiveTime)", strActiveTime);
                strJsonB = strJsonB.Replace("(RowCount)", intRowCount.ToString());
                strJsonB = strJsonB.Replace("(DurationFeeder)", lngDuration.ToString());
       
                lock (m_objLocker) // lock multi-thread => Message requested was not found in the queue specified
                {
                    ConfigurationOptions config = new ConfigurationOptions
                    {
                        EndPoints = {
                            { host1, port1 },
                            { host2, port2 }
                        },
                        ConnectTimeout = 1000,
                        AbortOnConnectFail = false
                    };
                    // Thiết lập thông tin kết nối Redis
                 
                    // Tạo đối tượng ConnectionMultiplexer
                    var connection = ConnectionMultiplexer.Connect(config);
                    // Lấy đối tượng Redis để publish dữ liệu
                    var redis = connection.GetDatabase();

                    // Publish dữ liệu đến kênh "mychannel"
                    redis.Publish(this.m_strChannelMonitor, strJsonB);
                    connection.Close();
                }

                return true;
            }
            catch (Exception ex)
            {
                this._errorLogger.LogError(ex);
                return false;
            }
        }
*/
        /// <summary>
        /// 2019-01-15 09:42:37 ngocta2
        /// send tinh trang cho center monitor
        /// </summary>
        /// <param name="appList"></param>
        /// <param name="statusData"></param>
        /// <returns></returns>
        public bool SendStatusToMonitor(AppList appList, string statusData)
        {
            try
            {
                // process ....

                // send ...
                this.SendStatusToMonitor(this._common.GetLocalDateTime(), this._common.GetLocalIp(), appList, statusData);

                return true;
            }
            catch (Exception ex)
            {
                this._errorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2019-01-15 10:05:09 ngocta2
        /// private function, send status cho monitor
        /// </summary>
        /// <param name="localDateTime">datetime cua server host app, ko phai datetime cua server host monitor</param>
        /// <param name="localIp"></param>
        /// <param name="appList"></param>
        /// <param name="statusData"></param>
        private void SendStatusToMonitor(string localDateTime, string localIp, AppList appList, string statusData)
        {
            try
            {
                // su dung ZeroMQ de send status ve monitor

            }
            catch (Exception ex)
            {
                this._errorLogger.LogError(ex);
            }
        }

        public void SendMessage(string strModule, string strMessage)
        {
            try
            {
                // su dung ZeroMQ de send status ve monitor

            }
            catch (Exception ex)
            {
                this._errorLogger.LogError(ex);
            }
        }
    }
}
