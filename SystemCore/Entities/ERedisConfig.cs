using System.Collections.Generic;
using SystemCore.SharedKernel;

namespace SystemCore.Entities
{
    /// <summary>
    /// cac config chung cua redis cho 6G
    /// </summary>
    public class ERedisConfig : CInstance
    {
        // consts
        public const string __SECTION_REDISCONFIG = "RedisConfig";
        public const string __SECTION_KEYSECTIONSUB = "KeySectionSub";
        public const int __MINUTES_IN_A_MONTH = 43830; //đủ time cho key sống 1 tháng

        // props
        public int DbNumber { get; set; }           // required
        public List<EHost> EndPoints { get; set; }  // required
        public string ServiceName { get; set; }     // not required
        public string ConnectionString { get; set; }// not required
        public int Version { get; set; }            // not required
        public string KeySectionSub { get; set; }   // required

        public EHost PubSub { get; set; }           // required
    }
}
