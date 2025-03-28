using CommonLib.Interfaces;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib.Implementations
{
    public class CRedisNewApp
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
        public IDatabase RC
        {
            get { return this.m_database; }
        }
        public CRedisNewApp(IS6GApp app, string Endpoint, int intDb)
        {
            try
            {
                this._app = app;

                m_lazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(Endpoint));
                this.m_database = m_lazyConnection.Value.GetDatabase(intDb);
            }
            catch(Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        public void SortedSetAddAsync(string key, string value, double score)
        {
            try
            {
                if (RC != null)
                {
                    RC.SortedSetAdd(key, value, score);
                }
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        public async Task SortedSetAddAsync2(string key, string value, double score)
        {
            try
            {
                if (RC != null)
                {
                    await RC.SortedSetAddAsync(key, value, score);
                }
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
                if (RC != null)
                {
                    // LLQ cache set
                    RC.StringSet(strKey, strValue_Set, TimeSpan.FromMinutes(intDuration));
                    RC.StringSet(backupKey, strValue_Set, TimeSpan.FromMinutes(intDuration));
                }
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
