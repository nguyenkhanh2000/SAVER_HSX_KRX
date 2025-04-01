using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SAVER_HNX_KRX_Lib.Models
{
    public class LS_Model
    {
        public long CN { get; set; } // lấy timestamp gán vào CN - mục đích tránh mất data khi value trùng nhau
        public string MT { get; set; }
        public double MP { get; set; }
        public long MQ { get; set; }
        public string SIDE { get; set; }
        //constructor
        public LS_Model()
        {
            SIDE = string.Empty;
        }
    }
}
