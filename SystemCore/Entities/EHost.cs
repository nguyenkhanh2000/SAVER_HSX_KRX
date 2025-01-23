using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    public class EHost
    {
        public string IP { get; set; }
        public int Port { get; set; }
        public int IsMaster { get; set; }
        public string ServiceName { get; set; }
    }
}
