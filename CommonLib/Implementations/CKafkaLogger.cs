using CommonLib.Interfaces;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Net;
using SystemCore.Entities;
using SystemCore.SharedKernel;
using SystemCore.Temporaries;

namespace CommonLib.Implementations
{
    public class CKafkaLogger : CBaseLogger, IErrorLogger, IDebugLogger, ISqlLogger, IInfoLogger, IDisposable
    {
        // consts
        public const string __LEVEL_ERROR = "Error";
        public const string __LEVEL_INFO  = "Info";
        public const string __LEVEL_SQL   = "Sql";
        public const string __LEVEL_DEBUG = "Debug";
        public const string __SUFFIX_PROCESS_NAME = ".exe";
        public const string __TYPE_KAFKA_ELK = "Kafka-ELK";

        // vars
        private readonly ProducerConfig _producerConfig;
        private readonly IProducer<Null, string> _producerBuilder;
        private readonly CHandCode _handCode;
        private readonly CCommon _common;
        private readonly ELog _logTemplate;

        // props
        public EKafkaLoggerConfig KafkaLoggerConfig { get; set; }
        //public EDeliveryResult DeliveryResult { get; set; }

        public CKafkaLogger(IConfiguration configuration, EKafkaLoggerConfig kafkaLoggerConfig):base(configuration, __TYPE_KAFKA_ELK)
        {
            this._common = new CCommon(null, null);
            this._handCode = new CHandCode();
            this._producerConfig = new ProducerConfig { BootstrapServers = kafkaLoggerConfig.BootstrapServers };
            this._producerBuilder = new ProducerBuilder<Null, string>(this._producerConfig).Build();
            this.KafkaLoggerConfig = kafkaLoggerConfig;

            this._logTemplate = new ELog()
            {
                Group = KafkaLoggerConfig.Group, // "Stock6G",
                App = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name,
                Timestamp = EGlobalConfig.DateTimeNow, // 2022-03-23 16:28:13.969
                Ip = this._common.GetIpShort(), // ["10.26.2.33"]
                Department = KafkaLoggerConfig.Department, //"FIT-BDRD"
                Host = Dns.GetHostName(), //"fit-ngocta2",
                //Level = level, //"Error",
                //Message = data, //"test error log",
                Username = this._common.GetProcessOwner(System.Diagnostics.Process.GetCurrentProcess().ProcessName + __SUFFIX_PROCESS_NAME),//"ngocta2",
                //CallStack = CCommon.GetDeepCaller(), //"Test_LogKafka_json"
            };

        }

        // ------------------------------------Common------------------------------------
        /// <summary>
        /// send data vao kafka >> ELK
        /// sau do co the tra cuu tai Kibana UI
        /// ko dung ProduceAsync vi qua cham
        /// </summary>
        /// <param name="level"></param>
        /// <param name="data"></param>
        public void LogToKafka(string level, string data)
        {
            ELog log = new ELog(this._logTemplate, level, data, CCommon.GetDeepCaller());
            string text = _handCode.Utf8Json_SerializeObject(log);
            var message = new Message<Null, string> { Value = text };
            this._producerBuilder.Produce(KafkaLoggerConfig.Topic, message);            
            DeliveryResult = new EDeliveryResult()
            {
                Message     = text,//dr.Value,
                Topic     = KafkaLoggerConfig.Topic,//dr.Topic,
                Partition = -1,//dr.Partition.Value,
                Offset    = -1//dr.Offset.Value
            };
        }
        // ------------------------------------/Common------------------------------------

        // ------------------------------------IInfoLogger------------------------------------
        public void LogInfo(string data)
        {
            data = $"Source  = {GetDeepCaller()}\r\nData    = {data}";
            this.LogToKafka(__LEVEL_INFO, data);
        }

        public void LogInfoContext(TExecutionContext ec, string data)
        {
            data = $"{GetDeepCaller()} = {data} ({ThreadId}) [{TaskId}] <<{ec.ElapsedMilliseconds}>> => {ec.Id}";
            this.LogToKafka(__LEVEL_INFO, data);
            this.WriteBufferMid(ec, data, true);
        }
        // ------------------------------------/IInfoLogger------------------------------------


        // ------------------------------------IDebugLogger------------------------------------
        public bool DebugMode { get { return this._debugFlag; } }
                

        //public ILogger Logger => throw new NotImplementedException();

        //public string FullLogPath => throw new NotImplementedException();

        //public string LogRootPath => throw new NotImplementedException();

        //public string LogDirPath => throw new NotImplementedException();

        public TExecutionContext LogDebug(string data)
        {
            string eId = Guid.NewGuid().ToString(); // execution id => chi xuat hien 1 lan execute đó, lần execute sau là id khac

            // chi log khi _flag set = true
            // log lien tuc se gay cham system
            if (this._debugFlag)
            {
                data = $"EId     = {eId} ({ThreadId}) [{TaskId}]\r\nSource  = {GetDeepCaller()}\r\nData    = {data}";
                this.LogToKafka(__LEVEL_DEBUG, data);
            }

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

        public void LogDebugContext(TExecutionContext ec, string data)
        {
            if (this._debugFlag)
            {
                data = $"Source  = {GetDeepCaller()}\r\nEId     = {ec.Id} ({ec.ThreadId}) [{ec.TaskId}] BeginF\r\n{ec.Buffer}EId     = {ec.Id} ({ec.ThreadId}) [{ec.TaskId}] EndF <<{ec.ElapsedMilliseconds}>>\r\nFinally = {data}";
                this.LogToKafka(__LEVEL_DEBUG, data);
            }
        }

        public void LogDebugShort(string data)
        {
            this.LogToKafka(__LEVEL_DEBUG, data);
        }

        public void LogDebugSub(TExecutionContext ec, string fileName, string data)
        {
            this.LogToKafka(__LEVEL_DEBUG, data);
        }

        public void SetDebugMode(bool flag)
        {
            this._debugFlag = flag;
        }

        public TExecutionContext WriteBufferBegin(string data, bool force = false)
        {
            // tao ec object
            TExecutionContext ec = new TExecutionContext()
            {
                Id = Guid.NewGuid().ToString(), // execution id => chi xuat hien 1 lan execute đó, lần execute sau là id khac
                Data = data,
                Timestamp = Timestamp,
                ThreadId = ThreadId,
                ThreadIdS = ThreadIdS,
                TaskId = TaskId,
                TaskIdS = TaskIdS,
                FunctionName = FunctionName
            };

            // log buffer
            this.WriteBufferMid(ec, data, force);

            // return EC object de debug tiep sau do
            return ec;

        }
        // ------------------------------------/IDebugLogger------------------------------------


        // ------------------------------------IErrorLogger------------------------------------
        public void LogError(Exception ex)
        {
            string data = $"Message     = {ex.Message}\r\nStackTrace  = {ex.StackTrace.TrimStart()}\r\nTargetSite  = {ex.TargetSite}";
            this.LogToKafka(__LEVEL_ERROR, data);
        }

        public void LogErrorContext(Exception ex, TExecutionContext ec)
        {
            string data = $"ContextId   = {ec.Id} ({ThreadId}) [{TaskId}]\r\nLastContext = {ec.LastContext}; ec.Data={ec.Data}\r\nMessage     = {ex.Message}\r\nStackTrace  = {ex.StackTrace.TrimStart()}\r\nTargetSite  = {ex.TargetSite}\r\n";
            this.LogToKafka(__LEVEL_ERROR, data);

            this.WriteBufferMid(ec, ex.Message, true);
        }
        // ------------------------------------/IErrorLogger------------------------------------


        // ------------------------------------ISqlLogger------------------------------------
        public void LogSql(string data)
        {
            data = $"Source  = {GetDeepCaller()}\r\nData    = {data}";
            this.LogToKafka(__LEVEL_SQL, data);
        }

        public void LogSqlContext(TExecutionContext ec, string data)
        {
            data = $"Source  = {GetDeepCaller()} => {ec.Id} ({ThreadId}) [{TaskId}]\r\nData    = {data}";
            this.LogToKafka(__LEVEL_SQL, data);

            // 2. log vao buffer
            this.WriteBufferMid(ec, data, true);
        }

        public void LogSqlSub(TExecutionContext ec, string fileName, string data)
        {
            this.LogSql(data);
        }

        //public void this.WriteBufferMid(TExecutionContext ec, string data, bool force = false)
        //{
        //    throw new NotImplementedException();
        //}
        // ------------------------------------/ISqlLogger------------------------------------

        /// <summary>
        /// da test, ko run dc vao day
        /// nhung msg van ve ELK, view log ra ok
        /// </summary>
        public new void Dispose()
        {
            this._producerBuilder.Flush();
        }

    }
}
