using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    /// <summary>
    /// dung cho kafka logger, send msg vao kafka xong co the nhin bao cao da send ok
    /// </summary>
    public class EDeliveryResult
    {
        public string Message { get; set; }
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
    }
}
