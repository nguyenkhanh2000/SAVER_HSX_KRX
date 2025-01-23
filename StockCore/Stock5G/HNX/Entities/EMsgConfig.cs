using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{
    public class EMsgConfig
    {
        public const string MSG_TYPE_SI = "SI"; // lay gia khop, khoi luong khop
        public const string MSG_TYPE_DI = "DI"; // lay gia khop, khoi luong khop
        public const string MSG_BLOCK_HEADER_INIT = "INIT";// lan dau tien send, gui cac danh sach ma theo thu tu abc
        public const string MSG_BLOCK_HEADER_DATA = "DATA";// lan sau tro di, gui cac data chinh
        public const string MSG_TYPE_I = "I"; // lay gia khop, khoi luong khop Index 

    }
}
