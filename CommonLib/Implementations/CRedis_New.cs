using CommonLib.Interfaces;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static CommonLib.Implementations.CModelPublic;

namespace CommonLib.Implementations
{
    public class CRedis_New
    {
        private readonly IS6GApp _app;
        private readonly Lazy<ConnectionMultiplexer> m_lazyConnection;
        private readonly Lazy<ConnectionMultiplexer> m_lazyConnectionFOX;
        private IDatabase m_database;
        private IDatabase m_databaseFox;

        private string m_strServiceName;
        private List<string> m_lstEndpoints;
        private ConnectionMultiplexer m_ConnectionMultiplexer;

        private const string TEMPLATE_REDIS_VALUE = "{\"Time\":\"(Now)\",\"Data\":[(RedisData)]}";
        private const string FORMAT_DATETIME_6 = "yyyy-MM-dd HH:mm:ss.fff";//8:39 AM Friday, April 01, 2016
        public IDatabase RC_1
        {
            get { return this.m_database; }
        }
        public IDatabase RC_2
        {
            get { return this.m_databaseFox; }
        }
        public CRedis_New(IS6GApp app, string Endpoint1, string Endpoint2, int intDb)
        {
            try
            {
                this._app = app;
                //LLQ
                m_lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(Endpoint1));
                this.m_database = m_lazyConnection.Value.GetDatabase(intDb);
                //FOX
                m_lazyConnectionFOX = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(Endpoint2));
                this.m_databaseFox = m_lazyConnectionFOX.Value.GetDatabase(intDb);
            }
            catch (Exception ex) 
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        public async Task SortedSetAddAsync(string key, string value, double score)
        {
            try
            {
                var taskLLQ = Task.Run(() =>
                {
                    try
                    {
                        if (RC_1 != null)
                        {
                            // LLQ cache set
                            RC_1.SortedSetAdd(key, value, score);
                        }
                    }
                    catch (Exception ex)
                    {
                        this._app.ErrorLogger.LogError(ex);
                    }
                });

                //FOX
                var taskFOX = Task.Run(() =>
                {
                    try
                    {
                        if (RC_2 != null)
                        {
                            // FOX cache set
                            RC_2.SortedSetAdd(key, value, score);
                        }
                    }
                    catch (Exception ex)
                    {
                        this._app.ErrorLogger.LogError(ex);
                    }
                });
                await Task.WhenAll(taskLLQ, taskFOX);
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        public bool SetCache(string strKey, string strValue, int intDuration)
        {
            try
            {
                string strValue_Set = this.AddHeaderFooter(strValue);
                string backupKey = "BACKUP:" + DateTime.Today.ToString("yyyy:MM:dd:") + strKey;
                //LLQ
                var taskRC1 = Task.Run(() =>
                {
                    try
                    {
                        if (RC_1 != null)
                        {
                            // LLQ cache set
                            RC_1.StringSet(strKey, strValue_Set, TimeSpan.FromMinutes(intDuration));
                            RC_1.StringSet(backupKey, strValue_Set, TimeSpan.FromMinutes(intDuration));
                        }
                    }
                    catch (Exception ex)
                    {
                        this._app.ErrorLogger.LogError(ex);
                    }
                });

                //FOX
                var taskRC2 = Task.Run(() =>
                {
                    try
                    {
                        if (RC_2 != null)
                        {
                            // FOX cache set
                            RC_2.StringSet(strKey, strValue_Set, TimeSpan.FromMinutes(intDuration));
                            RC_2.StringSet(backupKey, strValue_Set, TimeSpan.FromMinutes(intDuration));
                        }
                    }
                    catch (Exception ex)
                    {
                        this._app.ErrorLogger.LogError(ex);
                    }
                });
                Task.WhenAll(taskRC1, taskRC2).Wait();

                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
        public bool SetCacheBI(string strKey, string strValue, int intDuration)
        {
            try
            {
                // Backup dữ liệu theo định dạng khóa khác
                string backupKey = "BACKUP:" + DateTime.Today.ToString("yyyy:MM:dd:") + strKey;
                //LLQ
                var taskRC1 = Task.Run(() =>
                {
                    try
                    {
                        if (RC_1 != null)
                        {
                            RC_1.StringSet(strKey, strValue, TimeSpan.FromMinutes(intDuration));

                            RC_1.StringSet(backupKey, strValue, TimeSpan.FromMinutes(intDuration));
                        }
                    }
                    catch (Exception ex)
                    {
                        this._app.ErrorLogger.LogError(ex);
                    }
                });

                var taskRC2 = Task.Run(() =>
                {
                    try
                    {
                        if (RC_2 != null)
                        {
                            RC_2.StringSet(strKey, strValue, TimeSpan.FromMinutes(intDuration));

                            RC_2.StringSet(backupKey, strValue, TimeSpan.FromMinutes(intDuration));
                        }
                    }
                    catch (Exception ex)
                    {
                        this._app.ErrorLogger.LogError(ex);
                    }
                });
                Task.WhenAll(taskRC1, taskRC2).Wait();

                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }       
        private string AddHeaderFooter(string strRedisValue)
        {
            StringBuilder sb = new StringBuilder(TEMPLATE_REDIS_VALUE);
            try
            {
                sb = sb.Replace("(Now)", DateTime.Now.ToString(FORMAT_DATETIME_6));  //FORMAT_TIME
                sb = sb.Replace("(RedisData)", strRedisValue);
                string result = JsonConvert.SerializeObject(sb.ToString());
                return result;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "[]";
            }
        }
    }
}
