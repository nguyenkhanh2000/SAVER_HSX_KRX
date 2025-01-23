using BaseBrokerLib;
using BaseBrokerLib.Implementations;
using BaseSaverLib;
using BaseSaverLib.Implementations;
using BaseSaverLib.Interfaces;
using CommonLib.Implementations;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using PriceLib;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using SystemCore.Entities;
using static BaseBrokerLib.EBrokerConfig;

namespace SAVER_HNX_KRX
{
    public class Program
    {
        static public Mutex _mutex = new Mutex(true, "SAVER_HSX_KRX");
        static public CS6GApp _app;
        static public EBrokerConfig _brokerConfig;
        static public CBroker _broker;
        static public ESaverConfig _saverConfig;
        static public CSaver _saver;
        static public EPriceConfig _priceConfig;
        static public CRedisConfig _redisConfig;
        static public CMDDSHandler _handler;
        static public CMDDSRepository _repositionry;
        static public OracleConnection _connectionOracle;
        static public SqlConnection _connectionSql;

        static public CMonitor _monitor;
        static void Main(string[] args)
        {
            // start app
            Console.WriteLine("START APP => " + EGlobalConfig.DateTimeNow);

            // khong cho chay 2 instance
            if (!_mutex.WaitOne(TimeSpan.Zero, true))
            {
                Console.WriteLine("CAN NOT RUN 2 INSTANCES !!! => " + EGlobalConfig.DateTimeNow);
                return;
            }

            // read app config
            Init();

            Console.WriteLine($"_eBrokerConfig.Host: {_brokerConfig.Host}");
            Console.WriteLine($"_eBrokerConfig.Port: {_brokerConfig.Port}");
            Console.WriteLine($"_eBrokerConfig.Username: {_brokerConfig.Username}");
            Console.WriteLine($"_eBrokerConfig.Password: {_brokerConfig.Password}");
            Console.WriteLine($"_eBrokerConfig.QueueName: {_brokerConfig.QueueName}");
            Console.WriteLine($"_eBrokerConfig.ExchangeName: {_brokerConfig.ExchangeName}");
            Console.WriteLine($"_eBrokerConfig.RoutingKey: {_brokerConfig.RoutingKey}");
            Console.WriteLine($"_eBrokerConfig.Durable: {_brokerConfig.Durable}");

            Console.WriteLine();

            Console.WriteLine("PRESS ANY KEY TO CONTINUE...");
            Console.ReadLine();

            // Perform text input
            for (; ; )
            {
                string line = Console.ReadLine();
                if (string.IsNullOrEmpty(line))
                    break;
            }

            // Disconnect the client
            Console.WriteLine("Done!");

        }

        /// <summary>
        /// read file config
        /// init obj instances
        /// </summary>
        public static void Init()
        {
            try
            {
                // init config path
                CS6GFactory.SetConfigPath(AppDomain.CurrentDomain.BaseDirectory.TrimEnd('\\'));

                Console.WriteLine("SetConfigPath");

                _app = CS6GFactory.GetS6GAppInstance();


                Console.WriteLine("GetS6GAppInstance");

                _app.DebugLogger.SetDebugMode(false);  // khong debug lien tuc, ton res, chi debug 1 time ngan

                Console.WriteLine("SetDebugMode(false)");

                _brokerConfig = new EBrokerConfig();
                _app.Configuration.GetSection(EBrokerConfig.__SECTION_BROKER_INPUT_CONFIG).Bind(_brokerConfig);
                _broker = new CBroker(_app, BrokerConfigList.Input);

                _priceConfig = new EPriceConfig();
                _app.Configuration.GetSection(EPriceConfig.__SECTION_PRICECONFIG).Bind(_priceConfig);

                _redisConfig = new CRedisConfig();
                _app.Configuration.GetSection(CRedisConfig.__SECTION_PRICECONFIG).Bind(_redisConfig);

                CRedis_New _redis = new CRedis_New(_app, _redisConfig.Endpoints_1, _redisConfig.Endpoints_2, _redisConfig.Redis_DB);
                //CRedisClient redisview = new CRedisClient(_app, _redisConfig.Host_FOX, _redisConfig.Port_FOX, _redisConfig.Host_LLQ, _redisConfig.Port_LLQ);
                _monitor = new CMonitor(_redis);
                _repositionry = new CMDDSRepository(_app);
                _handler = new CMDDSHandler(_app, _repositionry, _redisConfig, _redis, _monitor);

                _saverConfig = new ESaverConfig();
                _app.Configuration.GetSection(ESaverConfig.__SECTION_SAVER_CONFIG).Bind(_saverConfig);
                _saver = new CSaver(_app, _saverConfig, _broker, _priceConfig, _handler, _redisConfig);


                Console.WriteLine($"_brokerConfig={_app.Common.SerializeObject(_brokerConfig)}");
                Console.WriteLine($"_saverConfig={_app.Common.SerializeObject(_saverConfig)}");

            }
            catch (Exception ex)
            {
                // log error
                _app.ErrorLogger.LogError(ex);

                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }
        }
    }
}
