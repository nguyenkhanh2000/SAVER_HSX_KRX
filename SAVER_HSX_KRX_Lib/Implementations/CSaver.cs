using BaseBrokerLib.Interfaces;
using BaseSaverLib.Interfaces;
using CommonLib.Implementations;
using CommonLib.Interfaces;
using Confluent.Kafka;
using MDDSCore.Messages;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using PriceLib;
using PriceLib.Implementations;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using SystemCore.Entities;

namespace BaseSaverLib.Implementations
{
	/// <summary>
	/// 2020-07-20 11:21:48 ngocta2
	/// saver app: 
	/// doc data tren msg queue de insert vao db
	/// co truy xuat vao SQL : MSSQL + Oracle nhung thong qua PriceService
	/// </summary>
	public class CSaver: ISaver
	{
		private readonly IS6GApp _app;
		private readonly IBroker _broker;
		private readonly ESaverConfig _saverConfig;
		private readonly EPriceConfig _priceConfig;
        private readonly IMDDSHandler _handler;
        private readonly CRedisConfig _redisConfig;
		
		//private readonly CRedisClient _redisClient;
        public CRedis_New m_RC;
        public System.Timers.Timer m_tmrProcessDataRedis;      // timer goi func xu ly du lieu insert REDIS (goi theo chu ky )
        public System.Timers.Timer m_tmrProcessDataDB;      // timer goi func xu ly du lieu insert DATABASE (goi theo chu ky )
        public System.Timers.Timer m_tmrProcessDataORACLE;      // timer goi func xu ly du lieu insert DATABASE (goi theo chu ky )
		private readonly CReaderBaseMQ m_crbMQ;
        public const string SQL_EXEC = "EXEC ";
        // props
        public ESaverConfig FeederConfig
		{
			get { return this._saverConfig; }
		}
        public EPriceConfig PriceConfig
        {
            get { return this._priceConfig; }
        }

        public long ReceivedBlockCount { get; set; }
		public long ReceivedByteCount { get; set; }
		public long SentBlockCount { get; set; }
		public long SentByteCount { get; set; }
        private readonly CRedis_New m_M5G;
        /// <summary>
        /// 2020-06-26 16:01:40 ngocta2
        /// constructor
        /// </summary>

        public CSaver(IS6GApp app, ESaverConfig saverConfig, IBroker broker, EPriceConfig priceConfig, IMDDSHandler handler,CRedisConfig redisConfig)
		{
			this._app				= app;
			this._saverConfig       = saverConfig;
			this._broker            = broker;
			this.ReceivedBlockCount = 0;
			this.ReceivedByteCount  = 0;
			this.SentBlockCount     = 0;
			this.SentByteCount      = 0;
			this._priceConfig		= priceConfig;
		    this._handler			= handler;
			this._redisConfig       = redisConfig;
          //  _redisClient = new CRedisClient(_app, _redisConfig.Host_FOX, _redisConfig.Port_FOX, _redisConfig.Host_LLQ, _redisConfig.Port_LLQ);
            //m_crbMQ = new CReaderBaseMQ(this._app, this._redisConfig);
            // this.InConfig();
            this.InitApp();
		}

		/// <summary>
		/// destructor
		/// </summary>
		~CSaver()
		{
			try
			{
				// free
				//if (this._pooledRedisClientManager != null)
				//	this._pooledRedisClientManager.Dispose();

				//if (!this._isMultithread)
				//{
				//	if (this._redisClient != null)
				//		this._redisClient.Dispose();
				//}
			}
			catch (Exception ex)
			{
				this._app.ErrorLogger.LogError(ex);
			}
		}
		// ==========================================================================================

		/// <summary>
		/// 2020-06-26 16:05:09 ngocta2
		/// cac logic can run khi init app
		/// </summary>
		/// <returns></returns>
		public bool InitApp()
		{
			try
			{
				if (this._broker != null) 
				{

                    GetMessages();

                    //this._broker.OnMessage = (receviedMsg) => { this.ReceiveMessageFromMessageQueue(receviedMsg); };

                    //this._broker.SetupOnReceivedEventHandler(); // SetupOnReceivedEventHandler chi goi 1 lan, da dung o day thi ko setup o tren
                }
                return true;
			}
			catch (Exception ex)
			{
				this._app.ErrorLogger.LogError(ex);
				return false;
			}
		}

		public void GetMessages()
		{
			try
			{
                List<(ulong DeliveryTag, string Message)> batchMessages = new List<(ulong, string)>();

                while (true)
                {
                    //CLog.LogEx("LogDequeuMsg.txt", "1.1 start");
                    batchMessages.Clear(); // Reset batch
                    Stopwatch stopwatch = Stopwatch.StartNew();


                    // Lấy message và thêm vào batch
                    while (batchMessages.Count < 500 /*KeyConfig.BatchSize*/ && stopwatch.Elapsed.TotalMilliseconds < 200 /*KeyConfig.TIME_DELAY*/)
                    {
                        //CLog.LogEx("LogDequeuMsg.txt", "2." + batchMessages.Count.ToString());
                        //CLog.LogEx("LogDequeuMsg.txt", "2." + stopwatch.Elapsed.ToString());
                        var result = this._broker.Channel.BasicGet(_broker.BrokerConfig.QueueName, autoAck: false);
                        if (result != null)
                        {
                            var message = Encoding.UTF8.GetString(result.Body.ToArray());
                            batchMessages.Add((result.DeliveryTag, message));
                        }
                        else
                        {
                            Thread.Sleep(50); // Đợi một thời gian ngắn nếu không có message
                        }
                    }

                    if (batchMessages.Count > 0)
                    {
                        try
                        {
                            // Xu ly data
                            ProcessorMsgFromRBQueue(batchMessages);
                            this._broker.Channel.BasicAck(deliveryTag: batchMessages[^1].DeliveryTag, multiple: true);
                            //batchMessages.Clear();
                        }
                        catch (Exception ex)
                        {
                            this._app.ErrorLogger.LogError(ex);
                            this._broker.Channel.BasicNack(deliveryTag: batchMessages[^1].DeliveryTag, multiple: true, requeue: true);
                        }
                    }

                    //CLog.LogEx("LogDequeuMsg.txt", "1.1 End");
                }
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        public void ProcessorMsgFromRBQueue(List<(ulong DeliveryTag, string Message)> messageBlocks)
        {
            try
            {
                string[] _msgArray = null;
                foreach(var (deliveryTag, message) in messageBlocks)
                {
                    if(_msgArray == null)
                    {
                        _msgArray = new string[1]; // lan dau thi create instance
                        _msgArray[0] = message;
                    }
                    else
                    {
                        Array.Resize(ref _msgArray, _msgArray.Length + 1); // lan sau thi tang size array + 1
                        _msgArray[_msgArray.Length - 1] = message;
                    }
                }
                if (_msgArray != null) 
                {
                    HandlerMsgArr(_msgArray);
                }
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        private void HandlerMsgArr(string[] arrMsg)
        {
            try
            {
                this._handler.BuildScriptSQL(arrMsg);
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        /// <summary>
        /// 2020-07-17 10:41:34 ngocta2
        /// nhan dc msg tu queue
        /// </summary>
        /// <param name="messageBlock"></param>
        /// <returns></returns>
        public bool ReceiveMessageFromMessageQueue(string messageBlock)
		{
			try
			{
				Console.WriteLine($"{EGlobalConfig.DateTimeNow} - ReceiveMessageFromMessageQueue messageBlock.length={messageBlock.Length}");

                // code run tren thread rieng
                //Task.Run( () =>{
                //      SendRawDataToWebApi(messageBlock);
                //});

                //this._handler.UpdateBulk(messageBlock);


                return true;
			}
			catch (Exception ex)
			{
				this._app.ErrorLogger.LogError(ex);
				return false;
			}
		}


		/// <summary>
		/// 2020-08-31 16:23:51 ngocta2
		/// fast speed
		/// var client = new HttpClient { BaseAddress = new Uri(baseUrl), DefaultRequestVersion = new Version(2, 0) };
		/// https://stackoverflow.com/questions/9145667/how-to-post-json-to-a-server-using-c => SUCCESS
		/// 
		/// // FAILED
		//using (var httpClient = new HttpClient() { DefaultRequestVersion = new Version(2, 0) })
		//{
		//	using (var request = new HttpRequestMessage(new HttpMethod(EGlobalConfig.__STRING_METHOD_POST), this._saverConfig.PriceServiceUrl)) //"http://localhost:31006/api/mdds/update"
		//	{
		//		request.Content = new StringContent(messageBlock);
		//		request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(EGlobalConfig.__STRING_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED); //"application/x-www-form-urlencoded"
		//		httpClient.SendAsync(request);
		//		///_ = httpClient.PostAsync(this._saverConfig.PriceServiceUrl, request.Content);
		//	}
		//}
		//using (var client = new HttpClient())
		//{
		//	client.PostAsync(this._saverConfig.PriceServiceUrl, new StringContent(messageBlock, Encoding.UTF8, EGlobalConfig.__STRING_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED));
		//}
		/// </summary>
		/// <param name="messageBlock">
		//		8=FIX.4.49=51935=d49=VNMGW56=9999934=152=20190517 09:14:26.12830001=STO20004=G4911=3851207=HO55=VN000000KMR230624=KMR30628=172930629=MIRAE Joint Stock Company30630=MIRAE Joint Stock Company20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000083225=231=1.0223=0.015=VND20020=13660000001149=999999999.01148=-999999999.0202=0.0965=N30631=1.01193=236=0.020013=4930.020014=0.020015=0.020016=0.0140=4930.020027=330642=30511=30301=2017010130614=220018=0030625=0.030635=NRM30636=SNE30637=NRM10=056
		//		8=FIX.4.49=51935=d49=VNMGW56=9999934=252=20190517 09:14:26.13430001=STO20004=T1911=3851207=HO55=VN000000KMR230624=KMR30628=172930629=MIRAE Joint Stock Company30630=MIRAE Joint Stock Company20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000083225=231=1.0223=0.015=VND20020=13660000001149=999999999.01148=-999999999.0202=0.0965=N30631=1.01193=236=0.020013=4930.020014=0.020015=0.020016=0.0140=4930.020027=330642=30511=30301=2017010130614=020018=0030625=0.030635=NRM30636=SNE30637=NRM10=062
		//		8=FIX.4.49=48835=d49=VNMGW56=9999934=352=20190517 09:14:26.13830001=STO20004=G2911=3851207=HO55=VN000000KPF030624=KPF30628=173830629=CTCP TV DA QT KPF30630=CTCP TV DA QT KPF20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000120225=231=1.0223=0.015=VND20020=13645000001149=0.01148=0.0202=0.0965=N30631=1.01193=236=0.020013=10600.020014=0.020015=0.020016=0.0140=10600.020027=330642=30511=30301=2016021830614=020018=0030625=0.030635=NRM30636=SNE30637=NRM10=014
		//		8=FIX.4.49=48435=d49=VNMGW56=9999934=452=20190517 09:14:26.14330001=STO20004=G3911=3851207=HO55=VN000000KPF030624=KPF30628=173830629=CTCP TV DA QT KPF30630=CTCP TV DA QT KPF20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000120225=231=1.0223=0.015=VND20020=13645000001149=0.01148=0.0202=0.0965=N30631=1.01193=236=0.020013=0.020014=0.020015=0.020016=0.0140=10600.020027=330642=30511=30301=2016021830614=020018=0030625=0.030635=NRM30636=SNE30637=NRM10=065
		/// </param>
		/// <returns></returns>
		public void  SendRawDataToWebApi(string messageBlock)
		{
			try
			{
				++this.ReceivedBlockCount;
				this.ReceivedByteCount += this._app.Common.GetStringLength(messageBlock);

                //Console.WriteLine($"{EGlobalConfig.DateTimeNow} - ReceivedBlockCount={ReceivedBlockCount}; ReceivedByteCount={ReceivedByteCount}");
                //Debug.WriteLine($"CSaver.ReceiveMessageFromMessageQueue.{EGlobalConfig.DateTimeNow} - ReceivedBlockCount = {this.ReceivedBlockCount}; ReceivedByteCount={ReceivedByteCount}");

                // SUCCESS
                var httpWebRequest = (HttpWebRequest)WebRequest.Create(this._saverConfig.PriceServiceUrl);
                //httpWebRequest.ProtocolVersion = new Version(2, 0); // Only HTTP/1.0 and HTTP/1.1 version requests are currently supported.
                httpWebRequest.ContentType = EGlobalConfig.__STRING_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED;
                httpWebRequest.Method = EGlobalConfig.__STRING_METHOD_POST;
               // httpWebRequest.Timeout = 50000;


                using (var streamWriter = new StreamWriter( httpWebRequest.GetRequestStream()))
                {
                    streamWriter.Write(messageBlock);


                }
				using (var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse())
                {
					using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
					{
						var result =  streamReader.ReadToEnd(); // {"Code":0,"Message":"","Data":null

					}
				}

                
          
                //string Response = "";
                //HttpResponseMessage HttpResponseMessage = null;
                //using (var httpClient = new HttpClient())
                //{
                //	httpClient.DefaultRequestHeaders.Accept.Clear();
                //	httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(EGlobalConfig.__STRING_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED));
                //	//application/xml



                //	var httpContent = new StringContent(messageBlock, Encoding.UTF8, EGlobalConfig.__STRING_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED);

                //	HttpResponseMessage = await httpClient.PostAsync(this._saverConfig.PriceServiceUrl, httpContent);

                //	if (HttpResponseMessage.StatusCode == HttpStatusCode.OK)
                //	{
                //		Response = HttpResponseMessage.Content.ReadAsStringAsync().Result;
                //	}
                //	else
                //	{
                //		Response = "Some error occured." + HttpResponseMessage.StatusCode.ToString();

                //		Console.WriteLine(Response);

                //	}
                //}

                //Console.WriteLine(Response);


            }
			catch (Exception ex)
			{
				this._app.ErrorLogger.LogError(ex);
				
				Console.WriteLine($"SendRawDataToWebApi.ex.Message={ex.Message}");
			}

			
		}
	}
}
