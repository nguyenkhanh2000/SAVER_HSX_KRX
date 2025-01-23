using Newtonsoft.Json;
using System.Runtime.Serialization;

namespace SystemCore.Entities
{
    /// <summary>
    /// send obj nay vao api cua ApiService de set PubSubProvider khac
    /// lenh nay co the send truc tiep vao api tu browser hoac tu monitor6g request sang
    /// 
    /// log set EPubSubProviderCommand can s end ve monitor6g de show ra console log (realtime)
    /// </summary>

    [JsonObject(MemberSerialization.OptIn)]
    public class ETransportProviderCommand
    {
        public const int __PROVIDER_ZMQ = 0;
        public const int __PROVIDER_REDIS = 1;

        public const string __FIELD_USERNMAE = "Username";
        public const string __FIELD_IP = "Ip";
        public const string __FIELD_PROVIDER = "Provider";
        public const string __FIELD_SETTIME = "SetTime";
        public const string __FIELD_TOKEN = "Token";
        public const string __SECRET = "??????";

        /// <summary>
        /// username nguoi login vao monitor6g de request
        /// </summary>
        [JsonProperty(PropertyName = __FIELD_USERNMAE)]
        [DataMember(Name = __FIELD_USERNMAE)]
        public string Username { get; set; } = "System";

        /// <summary>
        /// IP send request den, auto get ip = code
        /// </summary>
        [JsonProperty(PropertyName = __FIELD_IP)]
        [DataMember(Name = __FIELD_IP)]
        public string IP { get; set; } = "127.0.0.1";

        /// <summary>
        /// random string luu trong mem
        /// init hub thi lay token tu monitor
        /// monitor ko request dc thi dung token trong config file
        /// </summary>
        [JsonProperty(PropertyName = __FIELD_TOKEN)]
        [DataMember(Name = __FIELD_TOKEN)]
        public string Token { get; set; } = __SECRET;

        /// <summary>
        /// 0 - zeromq
        /// 1 - redis
        /// </summary>        
        [JsonProperty(PropertyName = __FIELD_PROVIDER)]
        [DataMember(Name = __FIELD_PROVIDER)]
        public int Provider { get; set; } = __PROVIDER_ZMQ; // __PROVIDER_REDIS;

        /// <summary>
        /// thoi diem set provider
        /// </summary>
        [JsonProperty(PropertyName = __FIELD_SETTIME)]
        [DataMember(Name = __FIELD_SETTIME)]
        public string SetTime { get; set; } = EGlobalConfig.DateTimeNow;

    }
}
