using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SAVER_HSX_KRX_Lib.Models
{
    public class PT_Side_Model
    {
        public string Symbol { get; set; }
        public Side_Data Data { get; set; }
    }
    public class Side_Data
    {
        public double MP { get; set; }
        public long MQ { get; set; }
    }
}
