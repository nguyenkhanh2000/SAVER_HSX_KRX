using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    /// <summary>
    /// luu thong tin ip
    /// </summary>
    public class EIP
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int NetworkInterfaceType { get; set; }
        public int OperationalStatus { get; set; }
        public long Speed { get; set; }
        public bool IsReceiveOnly { get; set; }
        public bool SupportsMulticast { get; set; }
        //------------------------------------------------------
        public string DnsSuffix { get; set; }
        public bool IsDnsEnabled { get; set; }
        public bool IsDynamicDnsEnabled { get; set; }

        public List<string> UnicastIPAddresses { get; set; }
        public List<string> MulticastAddresses { get; set; }
        public List<string> AnycastAddresses { get; set; }

        public List<string> GatewayAddresses { get; set; }
        public List<string> DnsAddresses { get; set; }
        public List<string> DhcpServerAddresses { get; set; }
    }
}
