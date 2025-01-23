using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock6G.Entities
{
    public class EAccount
    {
        public string LoginName { get; set; }           // 058C547797
        public string Password { get; set; }            // login password
        public string TradingPassword { get; set; }     // trading password: pass luu trong db hoac la token passcode 
        public string Source { get; set; }              // 1
        public string IpClient { get; set; }            // 59.153.243.132
        public string IpServer { get; set; }            // 192.168.2.11
        public string SessionToken { get; set; }        // 18b52c2d-3e10-4d57-ae97-b1fce83345a0
        public string Referer { get; set; }             // https://eztrade.fpts.com.vn
        public string UserAgent { get; set; }           // Mozilla/5.0 (iPhone; CPU iPhone OS 10_1 like Mac OS X) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0 Mobile/14B72c Safari/602.1
        public string Browser { get; set; }             // Safari10.0
        public string IsMobile { get; set; }            // Y
        public string BrowserName { get; set; }         // Safari
        public string BrowserVers { get; set; }         // 10.0
        public string ClientCode { get; set; }          // output/input truoc kia la 6 so, sau nay sang oracle thi la 10 so
        public string ClientName { get; set; }          // output
        public string SessionNo { get; set; }           // output
        public long ErrorCode { get; set; }             // output
        public string Message { get; set; }             // output
        public int CheckSession { get; set; }           // output - dang trong phien lam viec hay ko
        public int LoginStatus { get; set; }            // output - 0-login thanh cong; 1-login thanh cong,lan dau login, bi ep doi mat khau
        public int RsaToken { get; set; }         // output - 0-dung mat khau tinh, khong dung token; 1-dung token
        public int CheckTradingPassword { get; set; }   // output - 1-thanh cong; 0-that bai (ap dung chung cho ca 2 loai pass db/token)
        public int RsaResult { get; set; }        // output - ket qua tra ve tu ws rsa agent 2014 => http://172.16.0.xx:8888/fpts_ws_agent.asmx
                                                  //CODE_SUCCESS = 0,
                                                  //CODE_FAILED_ACCESS_DENIED = 1,
                                                  //CODE_FAILED_NEXT_PASSCODE_REQUIRED = 2,
                                                  //CODE_FAILED_NEW_PIN_REQUIRED = 5,
                                                  //CODE_FAILED_PIN_ACCEPTED = 6,
                                                  //CODE_FAILED_PIN_REJECTED = 7
        public string Jwt { get; set; }                 // xac thuc tat ca request vao ApiGateway (jwt value dat tai "Authorization" trong request header)
        public bool IsCheckSessionSuccess { get { return CheckSession == 1; } } // kiem tra session ok ?
        public bool IsFirstLogin { get { return LoginStatus == 1; } } // login lan dau thi bi ep doi mat khau
        public bool IsRsaTokenTradingPassword { get { return RsaToken == 1; } } // co su dung token ?
        public string RsaMessage
        { // ro nghia hon la chi tra code
            get
            {
                switch (RsaResult)
                {
                    case 0: return "SUCCESS";
                    case 1: return "FAILED_ACCESS_DENIED";
                    case 2: return "FAILED_NEXT_PASSCODE_REQUIRED";
                    case 5: return "FAILED_NEW_PIN_REQUIRED";
                    case 6: return "FAILED_PIN_ACCEPTED";
                    case 7: return "FAILED_PIN_REJECTED";
                    default: return "";
                };
            }
        }
        public int CheckPass2 { get; set; }           // output - 0: chưa xác thực pass2 lần đầu THÀNH CÔNG trong phiên gd; 1: đã xác thực pass2 lần đầu THÀNH CÔNG trong phiên gd [dùng cho khách p_reTrdPass1by1=0]
        public int TradingPass1By1 { get; set; }           // output - 1: Sử dụng mkgd cho từng lần đặt lệnh mua bán; 0: Sử dụng mkgd 1 lần cho cả phiên giao dịch

        public string OldPass { get; set; }
        public string NewPass { get; set; }
        public string OldTradePass { get; set; }
        public string NewTradePass { get; set; }
        public int IntResult { get; set; }
    }
}
