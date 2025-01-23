using CommonLib.Interfaces;
using Microsoft.Extensions.Configuration;
using MonitorCore.Interfaces;
using System;
using System.IO;
using System.Reflection;
using SystemCore.Entities;
using SystemCore.Interfaces;

namespace CommonLib.Implementations
{
    /// <summary>
    /// 2019-01-09 14:23:06 ngocta2
    /// factory class de tao ra cac singleton object (1 project chi duoc tao 1 instance)
    /// 1 project chỉ duoc co 1 S6GApp instance
    /// S6GApp = common + debug logger + error logger + sql logger + monitor + config (la file config rieng cua tung project => appsettings.json)
    /// </summary>
    static public class CS6GFactory
    {
        static private IConfiguration _configuration = null;
        static private CKafkaLogger _kafkaLogger     = null;
        static private IErrorLogger _errorLogger     = null;
        static private IDebugLogger _debugLogger     = null;
        static private ISqlLogger _sqlLogger         = null;
		static private IInfoLogger _infoLogger       = null;
        static private ICommon _common               = null;
        static private IHandCode _handCode           = null;
        static private IMonitor _monitor             = null;
        static private CS6GApp _app                  = null;
        static private object _locker                = new object(); // locker, tranh error voi multi-thread code
        static private string _configFolderPath      = null;
        static private string _configFileName        = null;        

        private const string __JSON_CONFIG_FILE = "appsettings.json";

        /// <summary>
        /// set path config
        /// </summary>
        /// <param name="configFolderPath">D:\Source\Repos\Stock6G\CommonLib.Tests</param>
        /// <param name="configFileName">appsettings.json</param>
        static public void SetConfigPath(string configFolderPath, string configFileName = null)
        {
            _configFolderPath = configFolderPath;
            _configFileName   = configFileName;
        }

		/// <summary>
		/// 2019-01-09 09:07:33 ngocta2
		/// fixed config
		/// 2019-01-09 14:46:12 ngocta2
		/// 1. chi duoc tao 1 instance
		/// 2. locker chong multi-thread tao nhieu instance
		/// 
		/// CS6GFactory.SetConfigPath(Directory.GetCurrentDirectory()); trong console app thi ra dung path nhung web app pool thi sai
		/// 
		/// https://stackoverflow.com/questions/43709657/how-to-get-root-directory-of-project-in-asp-net-core-directory-getcurrentdirect
		/// Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
		//using (FileStream stream = new FileStream($"D:\\temp4\\GetConfigInstance.txt", FileMode.Append, FileAccess.Write, FileShare.None, 4096, true))
		//using (StreamWriter sw = new StreamWriter(stream))
		//{
		//sw.WriteLine($"{EGlobalConfig.DateTimeNow} -- GetConfigInstance() -- _configFolderPath={_configFolderPath}");
		//}
		/// </summary>
		/// <returns></returns>
		static public IConfiguration GetConfigInstance()
        {
            lock (_locker)
            {
                if (_configuration == null)
                {
                    if (_configFolderPath == null)
					{
                        //_configFolderPath = Directory.GetCurrentDirectory(); //_configFolderPath=c:\windows\system32\inetsrv
                        // C:\Users\ngocta2\.nuget\packages\microsoft.testplatform.testhost\15.9.0\lib\netstandard1.5
                        //_configFolderPath = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location); //_configFolderPath=D:\temp\TAChartRealtimeService
                        //https://stackoverflow.com/questions/43709657/how-to-get-root-directory-of-project-in-asp-net-core-directory-getcurrentdirect
                        _configFolderPath = AppDomain.CurrentDomain.BaseDirectory.TrimEnd('\\'); // 2020-04-17 15:41:30 ngocta2 => D:\Source\Repos\Stock6G\BaseTransportLib.Tests\bin\Debug\netcoreapp2.2
                    }

                    if (_configFileName == null)
                        _configFileName = __JSON_CONFIG_FILE;

					_configuration = new ConfigurationBuilder()
                        .SetBasePath(_configFolderPath)
                        .AddJsonFile(_configFileName, optional: true, reloadOnChange: true)
                        .Build();
                }                    

                return _configuration;
            }
        }

        /// <summary>
        /// 2019-01-09 14:46:12 ngocta2
        /// 1. chi duoc tao 1 instance
        /// 2. locker chong multi-thread tao nhieu instance
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        static public IErrorLogger GetErrorLoggerInstance()
        {
            lock (_locker)
            {
                if (_errorLogger == null)
                {
                    IConfiguration configuration = CS6GFactory.GetConfigInstance();

                    _errorLogger = new CErrorLogger(configuration);
                }

                return _errorLogger;
            }
        }

        /// <summary>
        /// 2019-01-09 14:46:12 ngocta2
        /// 1. chi duoc tao 1 instance
        /// 2. locker chong multi-thread tao nhieu instance
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        static public IDebugLogger GetDebugLoggerInstance()
        {
            lock (_locker)
            {
                if (_debugLogger == null)
                {
                    IConfiguration configuration = CS6GFactory.GetConfigInstance();

                    _debugLogger = new CDebugLogger(configuration);
                }

                return _debugLogger;
            }
        }

        /// <summary>
        /// 2019-01-09 14:46:12 ngocta2
        /// 1. chi duoc tao 1 instance
        /// 2. locker chong multi-thread tao nhieu instance
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        static public ISqlLogger GetSqlLoggerInstance()
        {
            lock (_locker)
            {
                if (_sqlLogger == null)
                {
                    IConfiguration configuration = CS6GFactory.GetConfigInstance();

                    if (_kafkaLogger!=null)
                        _sqlLogger = _kafkaLogger;
                    else
                        _sqlLogger = new CSqlLogger(configuration);

                }

                return _sqlLogger;
            }
        }

		/// <summary>
		/// 2019-01-09 14:46:12 ngocta2
		/// 1. chi duoc tao 1 instance
		/// 2. locker chong multi-thread tao nhieu instance
		/// </summary>
		/// <param name="configuration"></param>
		/// <returns></returns>
		static public IInfoLogger GetInfoLoggerInstance()
		{
			lock (_locker)
			{
				if (_infoLogger == null)
				{
					IConfiguration configuration = CS6GFactory.GetConfigInstance();

					_infoLogger = new CInfoLogger(configuration);
				}

				return _infoLogger;
			}
		}

        static public CKafkaLogger GetKafkaLoggerInstance()
        {
            lock (_locker)
            {
                if (_kafkaLogger == null)
                {
                    IConfiguration configuration = CS6GFactory.GetConfigInstance();

                    EKafkaLoggerConfig kafkaLoggerConfig = new EKafkaLoggerConfig();
                    configuration.GetSection(EKafkaLoggerConfig.__SECTION_KAFKALOGGERCONFIG).Bind(kafkaLoggerConfig);

                    if (kafkaLoggerConfig.BootstrapServers != null)
                        _kafkaLogger = new CKafkaLogger(configuration, kafkaLoggerConfig);
                }

                return _kafkaLogger;
            }
        }

        /// <summary>
        /// 2019-01-09 14:46:12 ngocta2
        /// 1. chi duoc tao 1 instance
        /// 2. locker chong multi-thread tao nhieu instance
        /// </summary>
        /// <param name="cErrorLogger"></param>
        /// <param name="cDebugLogger"></param>
        /// <returns></returns>
        static public ICommon GetCommonInstance()
        {
            lock (_locker)
            {
                if (_common == null)
                {
                    _errorLogger = CS6GFactory.GetErrorLoggerInstance();
                    _debugLogger = CS6GFactory.GetDebugLoggerInstance();
                    _common      = new CCommon(_errorLogger, _debugLogger);
                }

                return _common;
            }
        }

        /// <summary>
        /// 2019-01-15 09:10:52 ngocta2
        /// 1. chi duoc tao 1 instance
        /// 2. locker chong multi-thread tao nhieu instance
        /// </summary>
        /// <returns></returns>
        static public IMonitor GetMonitorInstance()
        {
            lock (_locker)
            {
                if (_monitor == null)
                {
                    _errorLogger     = CS6GFactory.GetErrorLoggerInstance();
                    _debugLogger     = CS6GFactory.GetDebugLoggerInstance();
                    _configuration   = CS6GFactory.GetConfigInstance();
                    _common          = CS6GFactory.GetCommonInstance();
                    _monitor         = new CMonitor(_errorLogger, _debugLogger, _common, _configuration);

                }

                return _monitor;
            }
        }

        /// <summary>
        /// 2019-01-09 09:27:32 ngocta2
        /// singleton, ko duoc tao new instance moi lan call
        /// tao 2 instance S6GApp tuong duong tao 2 instance CDebugLogger khac nhau
        /// 2 instance khac nhau cung access 1 file thi error
        /// day la nguyen nhan neu tao new instance thi ghi log ko du data
        /// instance tao sau ko duoc access vao file log de append data
        /// </summary>
        /// <returns></returns>
        static public CS6GApp GetS6GAppInstance()
        {
            lock (_locker)
            {
                if (_app == null)
                {
                    _configuration   = CS6GFactory.GetConfigInstance();
                    _kafkaLogger     = CS6GFactory.GetKafkaLoggerInstance();

                    if (_kafkaLogger == null)
                    {
                        // ko co config KafkaLogger >> log ra disk qua Serilog
                        _errorLogger = CS6GFactory.GetErrorLoggerInstance();
                        _debugLogger = CS6GFactory.GetDebugLoggerInstance();
                        _sqlLogger   = CS6GFactory.GetSqlLoggerInstance();
                        _infoLogger  = CS6GFactory.GetInfoLoggerInstance();
                    }
                    else
                    {
                        // co config KafkaLogger >> send log vao Kafka >> ELK
                        _errorLogger = _kafkaLogger;
                        _debugLogger = _kafkaLogger;
                        _sqlLogger   = _kafkaLogger;
                        _infoLogger  = _kafkaLogger;
                    }
                    
                    _common          = CS6GFactory.GetCommonInstance();                    
                    _monitor         = CS6GFactory.GetMonitorInstance();
                    _handCode        = CS6GFactory.GetHandCodeInstance();                    

                    _app = new CS6GApp(_errorLogger, _debugLogger, _sqlLogger, _infoLogger, _common, _configuration, _monitor, _handCode);                    
                }

                return _app;
            }
        }

        /// <summary>
		/// 2019-01-09 14:46:12 ngocta2
		/// 1. chi duoc tao 1 instance
		/// 2. locker chong multi-thread tao nhieu instance
		/// </summary>
		/// <param name="cErrorLogger"></param>
		/// <param name="cDebugLogger"></param>
		/// <returns></returns>
		static public IHandCode GetHandCodeInstance()
        {
            lock (_locker)
            {
                if (_handCode == null)
                {
                    _handCode = new CHandCode();
                }

                return _handCode;
            }
        }
    }
}
