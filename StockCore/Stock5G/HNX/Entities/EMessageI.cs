using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{
    /// <summary>
    /// 2019-09-12 14:40 longht
    /// struct I
    /// </summary>
    public class EMessageI
    {
        //header
        public string f8 { get; set; } //begin string - Thông tin phiên bản do HNX quy định 
        public string f9 { get; set; } // BodyLength  - Độ dài message. Không tính các tag 8,9 35 
        public string f35 { get; set; } //MsgType - Loại message 
        public string f49 { get; set; } //SenderCompID  - ID người gửi. Nếu là message do HNX gửi thì giá trị là “HNX” 
        public string f52 { get; set; } //SendingTime  Thời gian gửi theo giờ UTC ( hay còn gọi là GMT) theo định dạng yyyyMMdd-HH:mm:ss 

        //body
        public string f1 { get; set; } //IDIndex - Int - ID của chỉ số 
        public string f2 { get; set; } //IndexCode - Mã chỉ số Mã chỉ số TRI , nếu gửi thông tin TRI Mã chỉ số DPI , nếu gửi thông tin DPI 
        public string f3 { get; set; } //Value - Giá trị chỉ số tại thời điểm hiện tại Giá trị TRI tại thời điểm hiện tại Giá trị DPI trong ngày 4 
        public string f4 { get; set; } //CalTime - Thời gian tính theo định dạng HH:mm:ssssss 
        public string f5 { get; set; } //Change - Giá trị thay đổi chỉ số hoặc TRI so với ngày hôm trước. Không đẩy ra với DPI 
        public string f6 { get; set; } //RatioChange - Tỷ lệ thay đổi chỉ số hoặc TRI. Không đẩy ra với DPI 
        public string f7 { get; set; } //TotalQtty - Tổng khối lượng giao dịch của khớp lệnh thông thường (lô chẵn). Không đẩy ra với DPI và TRI 
        public string f14 { get; set; } //TotalValue  - Tổng giá trị giao dịch của khớp lệnh thông thường (lô chẵn). Không đẩy ra với DPI và TRI 

        public string f19 { get; set; } //TradingDate  -  Ngày giao dịch hiện tại theo định dạng YYYYMMdd
        public string f21 { get; set; }// CurrentStatus  - Trạng thái của chỉ số: =1:bình thường 
        public string f22 { get; set; } // TotalStock  - Tổng số chứng khoán trong rổ. Không đẩy ra với DPI và TRI 
        public string f23 { get; set; } //PriorIndexVal - Giá trị index, TRI hoặc DPI ngày hôm trước 
        public string f24 { get; set; } //HighestIndex - Giá trị chỉ số, hoặc TRI cao nhất. Không đẩy ra với DPI 
        public string f25 { get; set; } //LowestIndex - Giá trị chỉ số hoặc TRI thấp nhất. Không đẩy ra với DPI 
        public string f26 { get; set; } //CloseIndex - Giá trị chỉ số hoặc TRI khi đóng cửa. Không đẩy ra với DPI 
        public string f27 { get; set; } //TypeIndex  - Loại chỉ số: = 0: Chỉ số thị trường = 1: Chỉ số bảng = 2: Chỉ số phức hợp = 3: Chỉ số ngành = 4: Chỉ số Top =  5: chỉ số TRI = 6: chỉ số DPI 18 
        public string f18 { get; set; } //IndexName  - Tên của chỉ số, tên TRI hoặc tên DPI 
    }
}
