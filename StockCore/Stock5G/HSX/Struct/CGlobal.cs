using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using static App.Metrics.AppMetricsConstants;

namespace StockCore.Stock5G.HSX.Struct
{
    public class CGlobal
    {
        public static long g_lngTickCount = 0;

        public const string MARKET_BEFORE_OPEN = "J";   // ko xai nua

        /*
      ControlCode	Time	TranDate
      P	90000	2015-05-26 14:45:59.010
      I	113000	2015-05-26 14:45:59.010
      F	130000	2015-05-26 14:45:59.010
      O	130000	2015-05-26 14:45:59.010
      A	143000	2015-05-26 14:45:59.010
      C	144500	2015-05-26 14:47:18.910
      K	150000	2015-05-26 15:01:28.943
      G	150543	2015-05-26 15:07:52.923
      J	150546	2015-05-26 15:07:52.923
      Z	150900	2015-05-26 15:10:28.913
      ************************************************************************* 
      ID	ControlCode	Time	TranDate	            Date
      3	G	        62031	2015-05-27 08:12:53.640	2015-05-27 08:12:53.640
      4	J	        62031	2015-05-27 08:12:53.640	2015-05-27 08:12:53.640
      */
        public const string MARKET_STAT_ATO = "P";
        public const string MARKET_STAT_ATC = "A";
        public const string MARKET_STAT_CON = "O"; // phien lien tuc
        public const string MARKET_STAT_CLO = "C"; // dong cua mainboard [14h45-15h00] day la luc dang giao dich trai phieu
        public const string MARKET_STAT_END = "K"; //  K => 150000	ghi data cuoi ngay vao REDIS (~ET file cua 4G)
        public const string ATO_PRICE = "0"; //"-100"; // {\"RowID\":\"6\",\"Info\":[[0,\"ALP\"],[1,4],[2,4.2],[3,3.8],[4,0],[5,0],[6,0],[7,0],[8,0],[9,3.8],[10,8930],[11,0],[12,0],[13,0],[14,4.2],[15,19400],[16,0],[17,0],[18,0],[19,0],[20,0],[21,0],[22,0],[23,0],[24,0],[25,0],[26,0],[27,0],[28,92800420]]}
        public const string ATC_PRICE = "0"; //"-200"; // js client can replace -2 thanh ATC


        /// <summary>
        ///+ C: Đóng cửa MainBoard
        ///+ F: Kết thúc nghỉ giữa đợt
        ///+ H: Ngưng giao dịch tất cả CK
        ///+ I: Bắt đầu nghỉ giữa đợt
        ///+ A: Bắt đầu đợt KL định kỳ đóng cửa
        ///+ K: Kết thúc đợt Runn-off
        ///+ N: Giao dịch trở lại của CK cụ thể
        ///+ O: Bắt đầu đợt KL lien tục
        ///+ P: Bắt đầu đợt KL định kỳ mở cửa
        ///+ R: Giao dịch trở lại tất cả CK 
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct MARKET_STAT
        {
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] ControlCode;		// Trạng thái (String, 1byt
            public int Time;                // Thời gian của máy chủ (Long, 4bytes)
        }

        /// <summary>
        /// FROOM.DAT - File Room cua nha dau tu nuoc ngoai
        /// </summary>
        //[StructLayout(LayoutKind.Sequential, Pack = 1)]
        //public struct FROOM
        //{
        //    public int StockNo;			    //193
        //    public double TotalRoom;	    //5388946 - Tổng room NĐTNN được phép mua
        //    public double CurrentRoom;	    //4311505 - Room còn lại NĐTNN được phép mua
        //}
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct FROOM
        {
            public int StockNo;			    //193 - Long ( 4 )
            public double TotalRoom;	    //5388946 - Double ( 8 ) = Tổng room NĐTNN được phép mua 
            public double CurrentRoom;	    //4311505 - Double ( 8 ) = Room còn lại NĐTNN được phép mua
            public double BuyVolume;	    //4311505 - Double ( 8 ) = Tổng khối lượng nước ngoài mua
            public double SellVolume;	    //4311505 - Double ( 8 ) = Tổng khối lượng nước ngoài bán
        }

        public struct FIRST_FROOM
        {
            public int StockNo;
            public long CurrentRoom;
        }

        // Last Index
        public struct LASTINDEX
        {
            public string Name;			//VNINDEX
            public double Value;	    //567.80
        }

        // Company Name
        public struct COMPANYNAME
        {
            public int CompanyId;
            public string StockCode;
            public int CodeID;
            public string Ex;
            public string CompanyVN;
            public string CompanyEN;
        }
        public struct MINISTRY
        {
            public int MinistryID;      // 4
            public string MinistryName; // Hàng hoá & Dịch vụ công nghiệp
            public string Stock_Code;   // ALT,BBS,BPC,BTC,CJC,CLC,CNH,AMD ...
        }

        //public struct VNXStruct
        //{
        //    public short StockNo;           // Mã chứng khoán dạng số [2bytes]
        //    [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
        //    public char[] StockSymbol;		// Mã chứng khoán dạng chuỗi [8bytes]
        //}
        public struct VNXLIST
        {
            public string VNXCode; // VN30
            public string SymbolCode;   // FPT, VNM, MSN, HAG...
        }

        /// <summary>
        /// 1 row thuc te tren UI cua client
        /// ap dung cho PB,LP,MW
        /// </summary>
        public struct FULL_ROW_QUOTE : ICloneable
        {
            public string Co; //{get; set; }  // 00 - code
            public string Re; //{get; set; }  // 01 - ref
            public string Ce; //{get; set; }  // 02 - ceiling
            public string Fl; //{get; set; }  // 03 - floor

            public string BQ4; //{get; set; } // 04 - buy quantity 4
            public string BP3; //{get; set; } // 05 - buy price 3
            public string BQ3; //{get; set; } // 06 - buy quantity 3
            public string BP2; //{get; set; } // 07 - buy price 2
            public string BQ2; //{get; set; } // 08 - buy quantity 2
            public string BP1; //{get; set; } // 09 - buy price 1
            public string BQ1; //{get; set; } // 10 - buy quantity 1

            public string MP; //{get; set; }  // 11 - match price
            public string MQ; //{get; set; }  // 12 - match quantity
            public string MC; //{get; set; }  // 13 - match change

            public string SP1; //{get; set; } // 14 - sell price 1
            public string SQ1; //{get; set; } // 15 - sell quantity 1
            public string SP2; //{get; set; } // 16 - sell price 2
            public string SQ2; //{get; set; } // 17 - sell quantity 2
            public string SP3; //{get; set; } // 18 - sell price 3
            public string SQ3; //{get; set; } // 19 - sell quantity 3
            public string SQ4; //{get; set; } // 20 - sell quantity 4

            public string TQ; //{get; set; }  // 21 - total quantity (NM)
            public string Op; //{get; set; }  // 22 - open (NM)
            public string Hi; //{get; set; }  // 23 - highest (NM)
            public string Lo; //{get; set; }  // 24 - lowest (NM)
            public string Av; //{get; set; }  // 25 - average (NM)
            public string FB; //{get; set; }  // 26 - foreign buy (quantity)
            public string FS; //{get; set; }  // 27 - foreign sell (quantity)
            public string FR; //{get; set; }  // 28 - foreign room (current)
            //-------------------
            public int SN; //{get; set; }  // 29 - hidden - StockNo
            public string ST; //{get; set; }  // 30 - hidden - StockType
            public string PO; //{get; set; }  // 31 - hidden - ProjectOpen
            public string Ri; //{get; set; }  // 32 - hidden - Rights 
            public string MPO; //{ get; set; } // 33 - hidden - MP old - truoc khi bi chuyen sang ProjectOpen
            public string MQO; //{ get; set; } // 34 - hidden - MQ old - truoc khi bi chuyen sang 0
            public string TQO; //{get; set; }  // 35 - hidden - total quantity old
            //==================
            //public void Reset()
            //{
            //    this.BP1 = "-1";
            //    this.SP1 = "-1";
            //}
            public FULL_ROW_QUOTE Clone()
            {
                return this;
            }
            object ICloneable.Clone()
            {
                return Clone();
            }
        }

        public struct FULL_ROW_INDEX
        {
            //MARKET_STAT
            public string STAT_ControlCode;
            public string STAT_Time;
            public string STAT_Date;
        }

        /// <summary>
        /// OS.DAT - Thông tin về Giá mở cửa của đợt khớp lệnh định kỳ
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct OS
        {
            public int StockNo;		// ID CK (Long, 4)        
            public int Price;		// Giá (Long, 4)
        }

        /// <summary>
        /// LS.DAT - File thông tin Mã xác nhận lệnh khớp+ Mã chứng khoán dạng số +Khối lượng khớp+Giá khớp+Bên mua/bán
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct LS
        {
            public long ConfirmNo;		    //int,	--	Long	4	Mã xác nhận lệnh khớp	Ghi thêm
            public int StockNo;		        //int ,	--	Long	4	Mã chứng khoán dạng số 	…
            public double MatchedVol;		//float,	--	Double	8	Khối lượng khớp	…
            public int Price;		        //int,	--	Long	4	Giá khớp	…
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Side;             // --	String	1	Bên mua/bán	…
        }

        /// <summary>
        /// LO.DAT - File thông tin Mã xác nhận lệnh khớp+ Mã chứng khoán dạng số +Khối lượng khớp+Giá khớp+Bên mua/bán lô lẻ
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct LO
        {
            public long ConfirmNo;		    //int,	--	Long	8	Mã xác nhận lệnh khớp	Ghi thêm
            public int StockNo;		        //int ,	--	Long	4	Mã chứng khoán dạng số 	…
            public double MatchedVol;		//float,	--	Double	8	Khối lượng khớp	…
            public int Price;		        //int,	--	Long	4	Giá khớp	…
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Side;             // --	String	1	Bên mua/bán	…
        }


#if READ_COVERED_WARRANT_20170420
        /// <summary>
        /// SECURITY.DAT - Chứa thông tin về giá, khối lượng giao dịch của tất cả chứng khóan niêm yết.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct SECURITY
        {
            public short StockNo;		    // Mã chứng khoán dạng số
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		// Mã chứng khoán dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] StockType;		// Lọai chứng khóan: + S: Cổ phiếu + D: Trái phiếu + U: Chứng chỉ quỹ
            public int Ceiling;		        // Giá trần
            public int Floor;		        // Giá sàn
            public double BigLotValue;		// Bỏ qua 

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 25)]
            public char[] SecurityName;		// Tên đây đủ của chứng khoán
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SectorNo;		    // Bỏ qua
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Designated;		// Bỏ qua
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SUSPENSION;		// CK bị tạm ngưng giao dịch:  + Null: Giao dịch bình thường + S: Bị tạm ngưng
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Delist;		    // CK bị hủy niêm yết: + Null: Giao dịch bình thường + D: Bị hủy niêm yết


            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] HaltResumeFlag;   // CK bị ngưng hoặc giao dịch trơ lại trong phiên giao dịch
            //                              + Null: Giao dịch bình thường
            //                              + H: Bị ngưng giao dịch trong phiên
            //                              + A: Bị ngưng giao dịch khớp lệnh trong phiên
            //                              + P: Bị ngưng giao dịch thỏa thuận trong phiên

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SPLIT;		    // CK thực hiện tách cổ phiếu:  
            //                              + Null: Không thực hiện 
            //                              + S: Thực hiện
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Benefit;		// CK thực hiện quyền và chia cổ tức
            //							  + Null: Không thực hiện
            //                            + A: Phát hành thêm & Cổ tức
            //                            + D: Chia cổ tức
            //                            + R: Thực hiện quyền
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Meeting;      // TCNY tổ chức đại hội cổ đông: 
            //                              + Null: Không 
            //                              + M: Tổ chức đại hội cổ đông    


            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Notice;       // TCNY bị yêu cầu cung cấp thong tin quan trong trong phiên giao dịch
            //                             + Null: Không
            //                             + P: Chờ thong tin cần cung cấp
            //                             + R: Đã nhận thong tin cung cấp

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] ClientIDRequest;	// Bỏ qua


            public short CouponRate;        //  Bỏ qua  

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
            public char[] IssueDate;		// Ngày phát hành
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
            public char[] MatureDate;		// Bỏ qua
            public int AvrPrice;		    // Giá bình quân gia quyền của các mức giá khớp
            public short ParValue;		    // Mệnh giá phát hành

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SDCFlag;		    // Bỏ qua
            public int PriorClosePrice;		// Giá đóng cửa gần nhất

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
            public char[] PriorCloseDate;	// Ngày giao dịch gần nhất
            public int ProjectOpen;		    // Giá tạm khớp trong đợt KL định kỳ
            public int OpenPrice;		    // Giá khớp mớ cửa
            public int Last;		        // Giá khớp
            public int LastVol;		        // Tổng khối lượng khớp
            public double LastVal;		    // Tổng giá trị khớp
            public int Highest;		        // Giá khớp cao nhất
            public int Lowest;		        // Giá Khớp thấp nhất
            public double Totalshares;		// Bỏ qua
            public double TotalValue;		// Bỏ qua
            public short AccumulateDeal;	// Bỏ qua
            public short BigDeal;		    // Bỏ qua
            public int BigVolume;		    // Bỏ qua
            public double BigValue;		    // Bỏ qua
            public short OddDeal;		    // Bỏ qua
            public int OddVolume;		    // Bỏ qua
            public double OddValue;		    // Bỏ qua    
            public int Best1Bid;		    // Giá đặt mua tốt nhất 1
            public int Best1BidVolume;		// Khối lượng tương ứng giá đặt mua 1
            public int Best2Bid;		    // Giá đặt mua tốt nhất 2
            public int Best2BidVolume;		// Khối lượng tương ứng giá đặt mua 2
            public int Best3Bid;		    // Giá đặt mua tốt nhất 3
            public int Best3BidVolume;		// Khối lượng tương ứng giá đặt mua 3
            public int Best1Offer;		    // Giá đặt bán tốt nhất 1
            public int Best1OfferVolume;	// Khối lượng tương ứng giá đặt bán 1
            public int Best2Offer;		    // Giá đặt bán tốt nhất 2
            public int Best2OfferVolume;	// Khối lượng tương ứng giá đặt bán 2
            public int Best3Offer;		    // Giá đặt bán tốt nhất 3
            public int Best3OfferVolume;	// Khối lượng tương ứng giá đặt bán 3
            public short BoardLost;		    // Bỏ qua

            // ---------------------- CW -----------------------------------
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] UnderlyingSymbol;		    // Chứng khoán cơ sở (sử dụng cho CW)               String 8
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 25)]
            public char[] IssuerName;		        // Tên tổ chức phát hành (sử dụng cho CW)           String 25
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] CoveredWarrantType;		// Loại chứng quyền (sử dụng cho CW)                String 1
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] MaturityDate;		        // Ngày hết hạn (sử dụng cho CW)                    String 8
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] LastTradingDate;          // Ngày giao dịch cuối cùng (sử dụng cho CW)        String 8
            public int ExercisePrice;		        // Giá thực hiện (sử dụng cho CW)                   Long 4
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 11)]
            public char[] ExerciseRatio;            // Tỷ lệ thực hiện. (sử dụng cho CW)                String 11
            public double ListedShare;              // Khối lượng CW niêm yết (sử dụng cho CW)          Double 8
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] OddLotFlag;               // Chứng khoán đang bị tạm dừng giao dịch hoặc giao dịch có được tiếp tục(đối với lô lẻ) string  1

            // ---------------------- /CW -----------------------------------

            // 2014-12-16 17:27:35 ngocta2
            // FAST-SPEED METHOD
            public override bool Equals(object obj)
            {
                //if (!(obj is STOCK_HCM))
                //  return false;

                var other = (SECURITY)obj;

                return StockNo == other.StockNo // short
                    && Ceiling == other.Ceiling // int
                    && Floor == other.Floor   // int

                    && ProjectOpen == other.ProjectOpen    // int
                    && OpenPrice == other.OpenPrice      // int
                    && Last == other.Last           // int
                    && LastVol == other.LastVol        // int
                    //&& LastVal      == other.LastVal        // double (ko tinh cai nay vi co the day la val PT)
                    && Highest == other.Highest        // int
                    && Lowest == other.Lowest         // int

                    && Best1Bid == other.Best1Bid             // int
                    && Best1BidVolume == other.Best1BidVolume       // int
                    && Best2Bid == other.Best2Bid             // int
                    && Best2BidVolume == other.Best2BidVolume       // int
                    && Best3Bid == other.Best3Bid             // int
                    && Best3BidVolume == other.Best3BidVolume       // int
                    && Best1Offer == other.Best1Offer             // int
                    && Best1OfferVolume == other.Best1OfferVolume       // int
                    && Best2Offer == other.Best2Offer             // int
                    && Best2OfferVolume == other.Best2OfferVolume       // int
                    && Best3Offer == other.Best3Offer             // int
                    && Best3OfferVolume == other.Best3OfferVolume       // int
                    ;
            }
        }
#else
        /// <summary>
        /// SECURITY.DAT - Chứa thông tin về giá, khối lượng giao dịch của tất cả chứng khóan niêm yết.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct SECURITY
        {
            public short StockNo;		    // Mã chứng khoán dạng số
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		// Mã chứng khoán dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] StockType;		// Lọai chứng khóan: + S: Cổ phiếu + D: Trái phiếu + U: Chứng chỉ quỹ
            public int Ceiling;		        // Giá trần
            public int Floor;		        // Giá sàn
            public double BigLotValue;		// Bỏ qua 

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 25)]
            public char[] SecurityName;		// Tên đây đủ của chứng khoán
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SectorNo;		    // Bỏ qua
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Designated;		// Bỏ qua
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SUSPENSION;		// CK bị tạm ngưng giao dịch:  + Null: Giao dịch bình thường + S: Bị tạm ngưng
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Delist;		    // CK bị hủy niêm yết: + Null: Giao dịch bình thường + D: Bị hủy niêm yết


            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] HaltResumeFlag;   // CK bị ngưng hoặc giao dịch trơ lại trong phiên giao dịch
            //                              + Null: Giao dịch bình thường
            //                              + H: Bị ngưng giao dịch trong phiên
            //                              + A: Bị ngưng giao dịch khớp lệnh trong phiên
            //                              + P: Bị ngưng giao dịch thỏa thuận trong phiên

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SPLIT;		    // CK thực hiện tách cổ phiếu:  
            //                              + Null: Không thực hiện 
            //                              + S: Thực hiện
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Benefit;		// CK thực hiện quyền và chia cổ tức
            //							  + Null: Không thực hiện
            //                            + A: Phát hành thêm & Cổ tức
            //                            + D: Chia cổ tức
            //                            + R: Thực hiện quyền
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Meeting;      // TCNY tổ chức đại hội cổ đông: 
            //                              + Null: Không 
            //                              + M: Tổ chức đại hội cổ đông    


            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Notice;       // TCNY bị yêu cầu cung cấp thong tin quan trong trong phiên giao dịch
            //                             + Null: Không
            //                             + P: Chờ thong tin cần cung cấp
            //                             + R: Đã nhận thong tin cung cấp

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] ClientIDRequest;	// Bỏ qua


            public short CouponRate;        //  Bỏ qua  

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
            public char[] IssueDate;		// Ngày phát hành
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
            public char[] MatureDate;		// Bỏ qua
            public int AvrPrice;		    // Giá bình quân gia quyền của các mức giá khớp
            public short ParValue;		    // Mệnh giá phát hành

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] SDCFlag;		    // Bỏ qua
            public int PriorClosePrice;		// Giá đóng cửa gần nhất

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
            public char[] PriorCloseDate;	// Ngày giao dịch gần nhất
            public int ProjectOpen;		    // Giá tạm khớp trong đợt KL định kỳ
            public int OpenPrice;		    // Giá khớp mớ cửa
            public int Last;		        // Giá khớp
            public int LastVol;		        // Tổng khối lượng khớp
            public double LastVal;		    // Tổng giá trị khớp
            public int Highest;		        // Giá khớp cao nhất
            public int Lowest;		        // Giá Khớp thấp nhất
            public double Totalshares;		// Bỏ qua
            public double TotalValue;		// Bỏ qua
            public short AccumulateDeal;	// Bỏ qua
            public short BigDeal;		    // Bỏ qua
            public int BigVolume;		    // Bỏ qua
            public double BigValue;		    // Bỏ qua
            public short OddDeal;		    // Bỏ qua
            public int OddVolume;		    // Bỏ qua
            public double OddValue;		    // Bỏ qua    
            public int Best1Bid;		    // Giá đặt mua tốt nhất 1
            public int Best1BidVolume;		// Khối lượng tương ứng giá đặt mua 1
            public int Best2Bid;		    // Giá đặt mua tốt nhất 2
            public int Best2BidVolume;		// Khối lượng tương ứng giá đặt mua 2
            public int Best3Bid;		    // Giá đặt mua tốt nhất 3
            public int Best3BidVolume;		// Khối lượng tương ứng giá đặt mua 3
            public int Best1Offer;		    // Giá đặt bán tốt nhất 1
            public int Best1OfferVolume;	// Khối lượng tương ứng giá đặt bán 1
            public int Best2Offer;		    // Giá đặt bán tốt nhất 2
            public int Best2OfferVolume;	// Khối lượng tương ứng giá đặt bán 2
            public int Best3Offer;		    // Giá đặt bán tốt nhất 3
            public int Best3OfferVolume;	// Khối lượng tương ứng giá đặt bán 3
            public short BoardLost;		    // Bỏ qua

            // 2014-12-16 17:27:35 ngocta2
            // FAST-SPEED METHOD
            public override bool Equals(object obj)
            {
                //if (!(obj is STOCK_HCM))
                //  return false;

                var other = (SECURITY)obj;

                return StockNo == other.StockNo // short
                    && Ceiling == other.Ceiling // int
                    && Floor == other.Floor   // int

                    && ProjectOpen == other.ProjectOpen    // int
                    && OpenPrice == other.OpenPrice      // int
                    && Last == other.Last           // int
                    && LastVol == other.LastVol        // int
                                                       //&& LastVal      == other.LastVal        // double (ko tinh cai nay vi co the day la val PT)
                    && Highest == other.Highest        // int
                    && Lowest == other.Lowest         // int

                    && Best1Bid == other.Best1Bid             // int
                    && Best1BidVolume == other.Best1BidVolume       // int
                    && Best2Bid == other.Best2Bid             // int
                    && Best2BidVolume == other.Best2BidVolume       // int
                    && Best3Bid == other.Best3Bid             // int
                    && Best3BidVolume == other.Best3BidVolume       // int
                    && Best1Offer == other.Best1Offer             // int
                    && Best1OfferVolume == other.Best1OfferVolume       // int
                    && Best2Offer == other.Best2Offer             // int
                    && Best2OfferVolume == other.Best2OfferVolume       // int
                    && Best3Offer == other.Best3Offer             // int
                    && Best3OfferVolume == other.Best3OfferVolume       // int
                    ;
            }
        }
#endif

        /// <summary>
        /// SECURITYOL.DAT - Chứa thông tin về giá, khối lượng giao dịch lô lẻ của chứng khoán
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct SECURITYOL
        {
            public short StockNo;		    // Mã chứng khoán dạng số
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		// Mã chứng khoán dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] StockType;		// Lọai chứng khóan: + S: Cổ phiếu + D: Trái phiếu + U: Chứng chỉ quỹ
            public int Ceiling;		        // Giá trần
            public int Floor;               // Giá sàn
            public int PriorClosePrice;		// Giá đóng cửa gần nhất

            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 25)]
            public char[] SecurityName;     // Tên đây đủ của chứng khoán




            public int LastOL;		        // Giá khớp
            public int LastOLVol;               // Tổng khối lượng khớp

            public int Best1Bid;		    // Giá đặt mua tốt nhất 1
            public int Best1BidVolume;		// Khối lượng tương ứng giá đặt mua 1
            public int Best2Bid;		    // Giá đặt mua tốt nhất 2
            public int Best2BidVolume;		// Khối lượng tương ứng giá đặt mua 2
            public int Best3Bid;		    // Giá đặt mua tốt nhất 3
            public int Best3BidVolume;		// Khối lượng tương ứng giá đặt mua 3
            public int Best1Offer;		    // Giá đặt bán tốt nhất 1
            public int Best1OfferVolume;	// Khối lượng tương ứng giá đặt bán 1
            public int Best2Offer;		    // Giá đặt bán tốt nhất 2
            public int Best2OfferVolume;	// Khối lượng tương ứng giá đặt bán 2
            public int Best3Offer;		    // Giá đặt bán tốt nhất 3
            public int Best3OfferVolume;    // Khối lượng tương ứng giá đặt bán 3


            // 2014-12-16 17:27:35 ngocta2
            // FAST-SPEED METHOD
            public override bool Equals(object obj)
            {
                //if (!(obj is STOCK_HCM))
                //  return false;

                var other = (SECURITYOL)obj;

                return StockNo == other.StockNo // short
                    && Ceiling == other.Ceiling // int
                    && Floor == other.Floor   // int


                    && LastOL == other.LastOL           // int
                    && LastOLVol == other.LastOLVol        // int
                                                           //&& LastVal      == other.LastVal        // double (ko tinh cai nay vi co the day la val PT

                    && Best1Bid == other.Best1Bid             // int
                    && Best1BidVolume == other.Best1BidVolume       // int
                    && Best2Bid == other.Best2Bid             // int
                    && Best2BidVolume == other.Best2BidVolume       // int
                    && Best3Bid == other.Best3Bid             // int
                    && Best3BidVolume == other.Best3BidVolume       // int
                    && Best1Offer == other.Best1Offer             // int
                    && Best1OfferVolume == other.Best1OfferVolume       // int
                    && Best2Offer == other.Best2Offer             // int
                    && Best2OfferVolume == other.Best2OfferVolume       // int
                    && Best3Offer == other.Best3Offer             // int
                    && Best3OfferVolume == other.Best3OfferVolume       // int
                    ;
            }
        }

        // ====================================================================================
        /// <summary>
        /// NEWLIST.DAT:  Thông tin về chứng khóan niêm yết mới. Tổng độ dài (Byte)	38		
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct NEWLIST
        {
            public short StockNo;			    // 974 (FPT) - Mã chứng khoán dạng số (Integer, 2bytes)
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		    // FPT       - Mã chứng khoán dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] StockType;		    // S         - Lọai chứng khóan: + S: Cổ phiếu + D: Trái phiếu + U: Chứng chỉ quỹ  + E Sản phẩm ETF	
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 25)]
            public char[] StockName;		    // FPT       - Tên đầy đủ của chứng khóan
            public short SectorNo;			    // 7200      - ? (Integer, 2bytes)
        }

        /// <summary>
        /// DELIST.DAT:  Thông tin về chứng khóan bị hủy niêm yết. Tổng độ dài (Byte)	38		
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct DELIST
        {
            public short StockNo;			    // 974 (FPT) - Mã chứng khoán dạng số (Integer, 2bytes)
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		    // FPT       - Mã chứng khoán dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] StockType;		    // S         - Lọai chứng khóan: + S: Cổ phiếu + D: Trái phiếu + U: Chứng chỉ quỹ  + E Sản phẩm ETF	
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 25)]
            public char[] StockName;		    // FPT       - Tên đầy đủ của chứng khóan
            public short SectorNo;			    // 7200      - ? (Integer, 2bytes)
        }


        /// <summary>
        /// LE.DAT - File thông tin giá khớp, khối lượng khớp, thời gian khớp trên MainBoard
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct LE
        {
            public int StockNo;			        // 974 (FPT) - Mã chứng khoán dạng số (Long, 4bytes)
            public int Price;			        // 7200      - Giá khớp (Long, 4bytes)
            public int AccumulatedVol;			// 29431     - Tổng khối lượng (Double, 8byte)
            public double AccumulatedVal;		// 21232     - Tổng giá trị (Double, 8byte)
            public int Highest;			        // 7300      - Giá khớp cao nhất (Long, 4bytes)
            public int Lowest;			        // 7150      - Giá khớp cao nhất (Long, 4bytes)
            public int Time;			        // 103115    - Thời gian khớp (Long, 4bytes)
        }


        /// <summary>
        /// TOTALMKT.DAT - File thông tin tổng hợp thị trường như VN-Index, tổng khối lượng, tổng giá trị của MainBoard và BigLotBoard.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct TOTALMKT
        {
            public int VNIndex;		        // 43092     - Chỉ số chứng khóan (int, 4bytes)
            public int TotalTrade;		    // 12273     - Tổng số lệnh được khớp (int, 8bytes)
            public double TotalShares;	    // 12278530  - Tổng khối lượng (Double, 8bytes)
            public double TotalValues;	    // 436692    - Tổng giá trị (Double, 8bytes)
            public double UpVolume;		    // 4778290   - Tồng KL của các CK tăng giá (Double, 8bytes)
            public double NoChangeVolume;	// 5882770   - Tồng KL của các CK đứng giá (Double, 8bytes)
            public double DownVolume;		// 1617470   - Tồng KL của các CK giảm giá (Double, 8bytes)
            public short Advances;		    // 32        - Tổng số CK tăng giá (Integer, 4bytes)
            public short Declines;		    // 46        - Tổng số CK giảm giá (Integer, 4bytes)
            public short Nochange;		    // 85        - Tổng số CK đứng giá (Integer, 4bytes)
            public int SET50Index;		    // 0         - Bỏ qua, không sử dụng
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] MarketID;		    // A         - Bỏ qua, không sử dụng
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Filler;		    // ""        - Bỏ qua, không sử dụng
            public int Time;		        // 94316     - Thời gian của máy chủ giao dịch (Long, 4bytes)
        }

        /// <summary>
        /// PUT_AD.DAT - File thông tin về các quảng cáo giao dịch thỏa thuận của trái phiếu và cổ phiếu trên BigLotBoard.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct PUT_AD
        {
            public short TradeID;		// 412           - Số hiệu giao dịch do mát chủ cấp (Integer, 2)
            public short StockNo;		// 3434 (TYA)    - Mã chứng khoán dạng số (Integer, 2)
            public int Vol;		        // 200000        - Khối lượng (Long, 4)
            public double Price;		// 10.2          - Giá (Double, 8)
            public int FirmNo;		    // 41            - Số hiệu Broker đăng quảng cáo (Long, 4) >> mã CTCK
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Side;		    // B             - Đăng mua/bán (String,1 )
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Board;		// B             - Bảng giao dịch  B: BigLotBoard.
            public int Time;		    // 103013        - Thời gian đăng quảng cáo (Long, 4)
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Flag;		    // A             - Tình trạng của tin đăng quảng cáo: + A: Quảng cáo được đăng + C: Quảng cáo bị hủy
        }

        /// <summary>
        /// PUT_EXEC.DAT - File Thông tin về lệnh giao dịch thỏa thuận được khớp.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct PUT_EXEC
        {
            public long ConfirmNo;		// 11    - Số hiệu giao dịch do máy chủ cấp (Long, 4)
            public short StockNo;		// 1146  - Mã chứng khoán dạng số (Integer, 2)
            public int Vol;		        // 50000 - Khối lượng (Long, 4)
            public int Price;		    // 4600  - Giá (Long, 4)
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Board;		// B     - Bảng giao dịch  B: BigLotBoard.
        }

        /// <summary>
        /// PUT_DC.DAT - Thông tin về lệnh khớp giao dịch thỏa thuận bị hủy. [chưa có trường hợp nào] 
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct PUT_DC
        {
            public long ConfirmNo;		// Số hiệu giao dịch do máy chủ cấp (Long, 4)
            public short StockNo;		// Mã chứng khoán dạng số (Integer, 2)
            public int Vol;		        // Khối lượng (Long, 4)
            public int Price;		    // Giá (Long, 4)
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 1)]
            public char[] Board;		// Bảng giao dịch  B: BigLotBoard.
        }


        /// <summary>
        /// YYYYMMDD_VNX.DAT: Thông tin tổng hợp chỉ số VNX .
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct VNX_MARKET_INDEX
        {
            public double IndexValue;          // 43092     - VNIndex (Long, 4bytes)
            public double TotalSharesAOM;     // 12278530  - Tổng khối lượng (Double, 8bytes)
            public double TotalValuesAOM;     // 436692    - Tổng giá trị (Double, 8bytes)
            public double TotalSharesPT;      // 12278530  - Tổng khối lượng (Double, 8bytes)
            public double TotalValuesPT;      // 436692    - Tổng giá trị (Double, 8bytes)
            public short Up;                // Short 2 Tổng số CK tăng giá …
            public short Down;              // Short 2 Tổng số CK giảm giá …
            public short NoChange;          // Short 2 Tổng số CK đứng giá …
            public short Ceiling;           // Short 2 Tổng số CK tăng trần
            public short Floor;             // Short 2 Tổng số CK giảm sàn 
            public int Time;                // Thời gian của máy chủ giao dịch (Long, 4bytes)
        }

        ///// <summary>
        ///// YYYYMMDD_VNINDEX.DAT: Thông tin tổng hợp chỉ số VNX .
        ///// </summary>
        //[StructLayout(LayoutKind.Sequential, Pack = 1)]
        //public struct VNX_MARKET_VNINDEX
        //{
        //    public int Index;          // 43092     - VNIndex (Long, 4bytes)
        //    public int TotalTrade;     //  - số lượng giao dịch (long, 4bytes)
        //    public double TotalShares;      //   - Tổng khối lượng (Double, 8bytes)
        //    public double TotalValues;     //    - Tổng giá trị (Double, 8bytes)
        //    public double UpVolume;                //  Tồng KL của các CK tăng giá(Double, 8bytes) …
        //    public double DownVolume;              //  Tồng KL của các CK giảm giá(Double, 8bytes) …
        //    public double NoChangeVolume;          // Tổng KL của các CK đứng giá (Double, 8bytes)
        //    public short Up;                // Short 2 Tổng số CK tăng giá …
        //    public short Down;              // Short 2 Tổng số CK giảm giá …
        //    public short NoChange;          // Short 2 Tổng số CK đứng giá …
        //    public int Time;                // Thời gian của máy chủ giao dịch (Long, 4bytes)
        //}


        /// <summary>
        /// YYYYMMDD_VNX.DAT: Thông tin tổng hợp chỉ số VNX .
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct VNX_MARKET_LIST
        {
            public short StockNo;           // Mã chứng khoán dạng số [2bytes]
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		// Mã chứng khoán dạng chuỗi [8bytes]
        }


        /// <summary>
        /// Thông tin tổng hợp iNAV
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct INAV
        {
            public short StockNo;           // Mã ETF dạng số
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] StockSymbol;		// Mã chứng khoán dạng chuỗi
            public int iNAV;                // Giá trị tài sản ròng tham chiếu
            public int Time;                // Thời gian của máy chủ giao dịch (Long, 4bytes)
        }

        /// <summary>
        /// Thông tin tổng hợp iIndex
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct IINDEX
        {
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 20)]
            public char[] iIndexSymbol;		// Mã iIndex dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 8)]
            public char[] ETFSymbol;		// Mã ETF dạng chuỗi
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 11)]
            public char[] IndexSymbol;		// Mã ETF dạng chuỗi
            public int iIndex;              // Chỉ số iIndex
            public int Time;                // Thời gian của máy chủ giao dịch (Long, 4bytes)
        }
    }
}
