using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    public class ELog
    {
        /// <summary>
        /// 21ffe633-e5e5-460a-b067-6bd95b90fca5
        /// </summary>
        public string CorrelationId { get; set; }

        /// <summary>
        /// 10.26.2.33
        /// </summary>
        public string Ip { get; set; }

        /// <summary>
        /// fit-ngocta2
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// FIT-BDRD
        /// </summary>
        public string Department { get; set; }

        /// <summary>
        /// Stock6G
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// HSXFeederApp
        /// </summary>
        public string App { get; set; }

        /// <summary>
        /// FIT-NGOCTA2\ngocta2
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// init app
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// 2022
        /// </summary>
        public string Timestamp { get; set; }

        /// <summary>        
        /// Debug       tim nguyen nhan bug
        /// Info        info it khi xay ra: begin app, end app, info quan trong
        /// Error       error
        /// SQL         sql script
        /// </summary>
        public string Level { get; set; }

        /// <summary>
        /// System.RuntimeMethodHandle->InvokeMethod=>CommonLib.Tests.UnitTest_CSystem->Test_GetCallStack=>
        /// </summary>
        public string CallStack { get; set; }

        public ELog()
        {
            CorrelationId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// su dung template de tranh phai exec lai cac function get ip,host,username,group,assembly ... rat ton res
        /// treo cpu 100%
        /// </summary>
        /// <param name="logTemplate"></param>
        /// <param name="level"></param>
        /// <param name="data"></param>
        /// <param name="callstack"></param>
        public ELog(ELog logTemplate, string level, string data, string callstack)
        {
            CorrelationId = Guid.NewGuid().ToString();

            Group      = logTemplate.Group;
            App        = logTemplate.App;
            Timestamp  = EGlobalConfig.DateTimeNow;
            Ip         = logTemplate.Ip;
            Department = logTemplate.Department;
            Host       = logTemplate.Host;
            Level      = level;
            Message    = data;
            Username   = logTemplate.Username;
            CallStack  = callstack;
        }
    }
}
