using BaseBrokerLib.Interfaces;
using CommonLib.Interfaces;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using static BaseBrokerLib.EBrokerConfig;

namespace BaseBrokerLib.Implementations
{
    /// <summary>
    /// 2020-07-15 09:51:54 ngocta2
    /// can message broker lam trung gian luu msg giua app1 va app2
    /// CBroker hien tai dung rabbitmq lam msg broker, tuong lai co the dung lib khac tuy y. vd : Apache Kafka
    /// </summary>
    public class CBroker : IBroker
    {
        private readonly EBrokerConfig _brokerConfig;
        private readonly IS6GApp _app;
        private ConnectionFactory _factory;
        private IConnection _connection;
        private RabbitMQ.Client.IModel _channel;
        private readonly EBrokerOutConfig _brokeroutConfig;
        private readonly EBrokerOutConfig _brokeroutIndexConfig;
        private readonly EBrokerOutConfig _brokeroutETConfig;
        private readonly EBrokerOutConfig _brokeroutG4Config;
        private readonly EBrokerOutConfig _brokeroutG7Config;
        private readonly EBrokerOutConfig _brokeroutG8Config;
        private readonly EBrokerOutConfig _brokeroutT1Config;
        private readonly EBrokerOutConfig _brokeroutT3Config;
        private readonly EBrokerOutConfig _brokeroutT2Config;
        private readonly EBrokerOutConfig _brokeroutT4Config;
        private readonly EBrokerOutConfig _brokeroutT6Config;
        private readonly EBrokerOutConfig _brokeroutT7Config;

        private readonly EBrokerOutConfig _brokeroutIndexHNXConfig;
        private IModel _channelIndexHNX;
        private IModel _channelIndex;
        private IModel _channelET;
        private IModel _channelG4;
        private IModel _channelG7;
        private IModel _channelG8;
        private IModel _channelT1;
        private IModel _channelT2;
        private IModel _channelT3;
        private IModel _channelT4;
        private IModel _channelT6;
        private IModel _channelT7;
        // props
        public IBroker.OnMessageEventHandler OnMessage { get; set; }
        public IModel Channel { get { return _channel; } }
        public EBrokerConfig BrokerConfig { get { return _brokerConfig; } }
        public EBrokerOutConfig OutConfig
        {
            get { return this._brokeroutConfig; }
        }
       
        //Queue cho Index
        public IModel ChannelIndex { get { return _channelIndex; } }
        public IModel ChannelIndexHNX { get { return _channelIndexHNX; } }
        public EBrokerOutConfig OutConfigIndex
        {
            get { return this._brokeroutIndexConfig; }
        }
        public EBrokerOutConfig OutConfigET
        {
            get { return this._brokeroutETConfig; }
        }


        /// <summary>
        /// constructor
        /// </summary>
        public CBroker(IS6GApp app, EBrokerConfig eBrokerConfig)
        {
            this._app = app;
            this._brokerConfig = eBrokerConfig;
            this.InitBroker();
        }
        public CBroker(IS6GApp s6GApp, BrokerConfigList configList = BrokerConfigList.None)
        {
            this._app = s6GApp;
            this._brokerConfig = new EBrokerConfig();
            this._brokeroutConfig = new EBrokerOutConfig();
            this._brokeroutIndexConfig = new EBrokerOutConfig();
            this._brokeroutETConfig = new EBrokerOutConfig();
            this._brokeroutG4Config = new EBrokerOutConfig();
            this._brokeroutG7Config = new EBrokerOutConfig();
            this._brokeroutG8Config = new EBrokerOutConfig();
            this._brokeroutT1Config = new EBrokerOutConfig();
            this._brokeroutT2Config = new EBrokerOutConfig();
            this._brokeroutT3Config = new EBrokerOutConfig();
            this._brokeroutT4Config = new EBrokerOutConfig();
            this._brokeroutT6Config = new EBrokerOutConfig();
            this._brokeroutT7Config = new EBrokerOutConfig();
            //this._brokeroutIndexHNXConfig = new EBrokerOutConfig();

            this._app.Configuration.GetSection(
                configList == BrokerConfigList.None ? EBrokerConfig.__SECTION_BROKER_CONFIG :
                (configList == BrokerConfigList.Input ? EBrokerConfig.__SECTION_BROKER_INPUT_CONFIG : (configList == BrokerConfigList.InputPT ? EBrokerConfig.__SECTION_BROKER_INPUT_CONFIG_PT : EBrokerConfig.__SECTION_BROKER_OUTPUT_CONFIG))
                ).Bind(this._brokerConfig);
            //this._app.Configuration.GetSection(
            //  EBrokerOutConfig.__SECTION_BROKER_OUTPUT_INDEX_HNX_CONFIG).Bind(this._brokeroutIndexHNXConfig);
            this._app.Configuration.GetSection(
                              EBrokerOutConfig.__SECTION_BROKER_OUTPUT_CONFIG).Bind(this._brokeroutConfig);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_INDEX_CONFIG).Bind(this._brokeroutIndexConfig);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_ET_CONFIG).Bind(this._brokeroutETConfig);
            // Add thêm mấu quue mới vào đâu LinhNH -
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_G4_CONFIG).Bind(this._brokeroutG4Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_G7_CONFIG).Bind(this._brokeroutG7Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_G8_CONFIG).Bind(this._brokeroutG8Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_T1_CONFIG).Bind(this._brokeroutT1Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_T2_CONFIG).Bind(this._brokeroutT2Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_T3_CONFIG).Bind(this._brokeroutT3Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_T4_CONFIG).Bind(this._brokeroutT4Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_T6_CONFIG).Bind(this._brokeroutT6Config);
            this._app.Configuration.GetSection(
               EBrokerOutConfig.__SECTION_BROKER_OUTPUT_T7_CONFIG).Bind(this._brokeroutT7Config);
            this.InitBroker();
        }


        /// <summary>
        /// 2020-07-14 16:36:02 ngocta2
        /// send msg vao rabbit queue
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueue(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                this._channel.BasicPublish(
                    exchange: this._brokeroutConfig.ExchangeName,
                    routingKey: this._brokeroutConfig.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        public bool SendMessageToQueueIndexHNX(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndexHNX.BasicPublish(
                    exchange: this._brokeroutIndexHNXConfig.ExchangeName,
                    routingKey: this._brokeroutIndexHNXConfig.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2020-07-14 16:00:30 ngocta2 
        /// setup OnRecevied event handler, chu y: co the receiver start truoc sender
        /// 
        /// Note that we declare the queue here as well. Because we might start the consumer before the publisher, 
        /// we want to make sure the queue exists before we try to consume messages from it.
        /// We're about to tell the server to deliver us the messages from the queue. Since it will push us 
        /// messages asynchronously, we provide a callback. That is what EventingBasicConsumer.Received event handler does.
        /// </summary>
        /// <returns></returns>
        public bool SetupOnReceivedEventHandler()
        {
            try
            {
                this._channel.QueueDeclare(
                        queue: _brokerConfig.QueueName,   //"hello"
                        durable: Convert.ToBoolean(this._brokerConfig.Durable),
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                var consumer = new EventingBasicConsumer(this._channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    //Console.WriteLine(" [x] Received {0}", message); // debug
                    OnMessage(message);
                };
                this._channel.BasicConsume(
                        queue: _brokerConfig.QueueName, //"hello"
                        autoAck: true,
                        consumer: consumer);

                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2020-07-15 09:20:00 ngocta2
        /// init broker obj
        /// chu y: can bind exchange/route voi queue name ko thi send mai ko vao queue
        /// </summary>
        /// <returns></returns>
        public bool InitBroker()
        {
            try
            {
                this._factory = new ConnectionFactory()
                {
                    HostName = this._brokerConfig.Host,
                    Port = this._brokerConfig.Port,
                    UserName = this._brokerConfig.Username,
                    Password = this._brokerConfig.Password
                };
                this._connection = this._factory.CreateConnection();
                this._channel = this._connection.CreateModel();
                this._channel.ExchangeDeclare(
                    this._brokerConfig.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokerConfig.Durable));

                this._channel.QueueDeclare(
                    queue: this._brokerConfig.QueueName,
                    durable: Convert.ToBoolean(this._brokerConfig.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                //this._channel.ExchangeDeclare(
                //	this._eBrokerConfig.ExchangeName, 
                //	ExchangeType.Direct,
                //	Convert.ToBoolean(this._eBrokerConfig.Durable));

                // From	"ExchangeMDDSHSX" voi Routing key "RoutingMDDSHSX" thi send vao queue "QueueMDDSHSX"
                this._channel.QueueBind(
                    this._brokerConfig.QueueName,
                    this._brokerConfig.ExchangeName,
                    this._brokerConfig.RoutingKey
                    );
                //Queue dành cho INDEX HNX
                //this._channelIndexHNX = this._connection.CreateModel();
                //this._channelIndexHNX.ExchangeDeclare(
                //    this._brokeroutIndexHNXConfig.ExchangeName,
                //    ExchangeType.Direct,
                //    Convert.ToBoolean(this._brokeroutIndexHNXConfig.Durable));

                //this._channelIndexHNX.QueueDeclare(
                //    queue: this._brokeroutIndexHNXConfig.QueueName,
                //    durable: Convert.ToBoolean(this._brokeroutIndexHNXConfig.Durable), // false, // 
                //    exclusive: false,
                //    autoDelete: false,
                //    arguments: null);

                //this._channelIndexHNX.QueueBind(
                //    this._brokeroutIndexHNXConfig.QueueName,
                //    this._brokeroutIndexHNXConfig.ExchangeName,
                //    this._brokeroutIndexHNXConfig.RoutingKey
                    //);

                //Queue dành cho Index
                this._channelIndex = this._connection.CreateModel();
                this._channelIndex.ExchangeDeclare(
                    this._brokeroutIndexConfig.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutIndexConfig.Durable));

                this._channelIndex.QueueDeclare(
                    queue: this._brokeroutIndexConfig.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutIndexConfig.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelIndex.QueueBind(
                    this._brokeroutIndexConfig.QueueName,
                    this._brokeroutIndexConfig.ExchangeName,
                    this._brokeroutIndexConfig.RoutingKey
                    );

                //Queue giành cho ET
                this._channelET = this._connection.CreateModel();
                this._channelET.ExchangeDeclare(
                    this._brokeroutETConfig.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutETConfig.Durable));

                this._channelET.QueueDeclare(
                    queue: this._brokeroutETConfig.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutETConfig.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelET.QueueBind(
                    this._brokeroutETConfig.QueueName,
                    this._brokeroutETConfig.ExchangeName,
                    this._brokeroutETConfig.RoutingKey
                    );

                // LinhNH 
                //Queue giành cho g4
                this._channelG4 = this._connection.CreateModel();
                this._channelG4.ExchangeDeclare(
                    this._brokeroutG4Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutG4Config.Durable));

                this._channelG4.QueueDeclare(
                    queue: this._brokeroutG4Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutG4Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelG4.QueueBind(
                    this._brokeroutG4Config.QueueName,
                    this._brokeroutG4Config.ExchangeName,
                    this._brokeroutG4Config.RoutingKey
                    );

                //Queue giành cho G7
                this._channelG7 = this._connection.CreateModel();
                this._channelG7.ExchangeDeclare(
                    this._brokeroutG7Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutG7Config.Durable));

                this._channelG7.QueueDeclare(
                    queue: this._brokeroutG7Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutG7Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelG7.QueueBind(
                    this._brokeroutG7Config.QueueName,
                    this._brokeroutG7Config.ExchangeName,
                    this._brokeroutG7Config.RoutingKey
                    );

                //Queue giành cho G8
                this._channelG8 = this._connection.CreateModel();
                this._channelG8.ExchangeDeclare(
                    this._brokeroutG8Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutG8Config.Durable));

                this._channelG8.QueueDeclare(
                    queue: this._brokeroutG8Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutG8Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelG8.QueueBind(
                    this._brokeroutG8Config.QueueName,
                    this._brokeroutG8Config.ExchangeName,
                    this._brokeroutG8Config.RoutingKey
                    );

                //Queue giành cho T1
                this._channelT1 = this._connection.CreateModel();
                this._channelT1.ExchangeDeclare(
                    this._brokeroutT1Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutT1Config.Durable));

                this._channelT1.QueueDeclare(
                    queue: this._brokeroutT1Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutT1Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelT1.QueueBind(
                    this._brokeroutT1Config.QueueName,
                    this._brokeroutT1Config.ExchangeName,
                    this._brokeroutT1Config.RoutingKey
                    );

                //Queue giành cho T2
                this._channelT2 = this._connection.CreateModel();
                this._channelT2.ExchangeDeclare(
                    this._brokeroutT2Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutT2Config.Durable));

                this._channelT2.QueueDeclare(
                    queue: this._brokeroutT2Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutT2Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelT2.QueueBind(
                    this._brokeroutT2Config.QueueName,
                    this._brokeroutT2Config.ExchangeName,
                    this._brokeroutT2Config.RoutingKey
                    );

                //Queue giành cho T3
                this._channelT3 = this._connection.CreateModel();
                this._channelT3.ExchangeDeclare(
                    this._brokeroutT3Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutT3Config.Durable));

                this._channelT3.QueueDeclare(
                    queue: this._brokeroutT3Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutT3Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelT3.QueueBind(
                    this._brokeroutT3Config.QueueName,
                    this._brokeroutT3Config.ExchangeName,
                    this._brokeroutT3Config.RoutingKey
                    );

                //Queue giành cho T4
                this._channelT4 = this._connection.CreateModel();
                this._channelT4.ExchangeDeclare(
                    this._brokeroutT4Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutT4Config.Durable));

                this._channelT4.QueueDeclare(
                    queue: this._brokeroutT4Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutT4Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelT4.QueueBind(
                    this._brokeroutT4Config.QueueName,
                    this._brokeroutT4Config.ExchangeName,
                    this._brokeroutT4Config.RoutingKey
                    );

                //Queue giành cho T6
                this._channelT6 = this._connection.CreateModel();
                this._channelT6.ExchangeDeclare(
                    this._brokeroutT6Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutT6Config.Durable));

                this._channelT6.QueueDeclare(
                    queue: this._brokeroutT6Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutT6Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelT6.QueueBind(
                    this._brokeroutT6Config.QueueName,
                    this._brokeroutT6Config.ExchangeName,
                    this._brokeroutT6Config.RoutingKey
                    );

                //Queue giành cho T7
                this._channelT7 = this._connection.CreateModel();
                this._channelT7.ExchangeDeclare(
                    this._brokeroutT7Config.ExchangeName,
                    ExchangeType.Direct,
                    Convert.ToBoolean(this._brokeroutT7Config.Durable));

                this._channelT7.QueueDeclare(
                    queue: this._brokeroutT7Config.QueueName,
                    durable: Convert.ToBoolean(this._brokeroutT7Config.Durable), // false, // 
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                this._channelT7.QueueBind(
                    this._brokeroutT7Config.QueueName,
                    this._brokeroutT7Config.ExchangeName,
                    this._brokeroutT7Config.RoutingKey
                    );
                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }

        }
        /// <summary>
        /// 2023-03-17 LinhNH
        /// send msg vao rabbit queue
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueueIndex(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutIndexConfig.ExchangeName,
                    routingKey: this._brokeroutIndexConfig.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-03-21 LinhNH
        /// send msg vao rabbit queue
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueueET(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelET.BasicPublish(
                    exchange: this._brokeroutETConfig.ExchangeName,
                    routingKey: this._brokeroutETConfig.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMesvsageToQueueIndex(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutIndexConfig.ExchangeName,
                    routingKey: this._brokeroutIndexConfig.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueueLoLe_G4(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutG4Config.ExchangeName,
                    routingKey: this._brokeroutG4Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueueBuyIn_G7(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutG7Config.ExchangeName,
                    routingKey: this._brokeroutG7Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueueSellIn_G8(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutG8Config.ExchangeName,
                    routingKey: this._brokeroutG8Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueuePT_T1(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutT1Config.ExchangeName,
                    routingKey: this._brokeroutT1Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueuePT_T2(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutT2Config.ExchangeName,
                    routingKey: this._brokeroutT2Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueuePT_T3(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutT3Config.ExchangeName,
                    routingKey: this._brokeroutT3Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueuePT_T4(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutT4Config.ExchangeName,
                    routingKey: this._brokeroutT4Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueuePT_T6(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutT6Config.ExchangeName,
                    routingKey: this._brokeroutT6Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }

        /// <summary>
        /// 2023-09-05 LinhNH
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool SendMessageToQueuePT_T7(string messageBlock)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(messageBlock);
                //Encoding.UTF8.GetBytes(messageBlock);
                this._channelIndex.BasicPublish(
                    //exchange		: "ExchangeHSXConvertUdp", "ExchangeQuoteFeederHsxExUdp",
                    //routingKey		: "RoutingHSXConvertUdp","RoutingQuoteFeederHsxExUdp",
                    exchange: this._brokeroutT7Config.ExchangeName,
                    routingKey: this._brokeroutT7Config.RoutingKey,
                    basicProperties: null,
                    body: body);


                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
    }
}

/*
try
{
	return true;
}
catch (Exception ex)
{
	this._s6GApp.ErrorLogger.LogError(ex);
	return false;
}
*/
