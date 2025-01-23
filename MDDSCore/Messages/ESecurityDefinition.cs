using Newtonsoft.Json;
using System;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2019-11-28 14:58:10 ngocta2
	/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
	/// <para><i>no=1; name=Security Definition; type=d; Batch</i></para>
	/// <para><b>Phân phối dữ liệu tham chiếu của mỗi chứng khoán.</b></para>
	/// <para><b>Thông tin này cung cấp tất cả các thông tin tham khảo về một chứng khoán và được cung cấp trước khi thị trường mở cửa.</b></para>   
	/// </summary>
	//  raw: 8=FIX.4.49=51935=d49=VNMGW56=9999934=152=20190517 09:14:26.12830001=STO20004=G4911=3851207=HO55=VN000000KMR230624=KMR30628=172930629=MIRAE Joint Stock Company30630=MIRAE Joint Stock Company20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000083225=231=1.0223=0.015=VND20020=13660000001149=999999999.01148=-999999999.0202=0.0965=N30631=1.01193=236=0.020013=4930.020014=0.020015=0.020016=0.0140=4930.020027=330642=30511=30301=2017010130614=220018=0030625=0.030635=NRM30636=SNE30637=NRM10=056
	[JsonObject(MemberSerialization.OptIn)]
    public class ESecurityDefinition : EBase
	{

        /// <summary>
        /// d = Security Definition
        /// </summary>
        public const string __MSG_TYPE = __MSG_TYPE_SECURITY_DEFINITION;


        /// <summary>
        /// 2019-11-28 14:58:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30001; required=Y; format=String; length=3</i></para>
        /// <para><b>ID xác định các thị trường</b></para>
        /// <para>
        ///  Sở giao dịch chứng khoán Tp Hồ Chí Minh <br></br>
        /// - STO: Thị trường cổ phiếu<br></br> 
        /// - BDO: Thị trường trái phiếu<br></br> 
        /// - RPO: Thị trường Repo
        /// </para>
        /// <para>
        ///  Sở giao dịch chứng khoán Hà Nội<br></br>
        /// - STX: Thị trường cổ phiếu<br></br> 
        /// - BDX: Thị trường trái phiếu chính phủ<br></br> 
        /// - DVX: Thị trường phái sinh
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30001, Order = 8)]
        public string MarketID { get; set; }

        /// <summary>
        /// 2019-11-28 14:58:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20004; required=Y; format=String; length=2</i></para>
        /// <para><b>ID Bảng giao dịch</b></para>
        /// <para>
        /// G1 : Chính(Main)<br></br>
        /// G2 : Trước giờ giao dịch(Pre Open)<br></br>
        /// G3 : Sau giờ giao dịch(Post Close)<br></br>
        /// G4 : Lô lẻ(Odd lot)<br></br>
        /// G7 : Mua bắt buộc(Buy-in)<br></br>
        /// G8 : Bán bắt buộc(Sell-out)<br></br>
        /// T1 : Thỏa thuận(regular)<br></br>
        /// T4 : Thỏa thuận lô lẻ(regular for Odd lot)<br></br>
        /// T2 : Thỏa thuận trước giờ giao dịch(pre)<br></br>
        /// T3 : Thỏa thuận sau giờ giao dịch(post)<br></br>
        /// T5 : Thỏa thuận sau giờ giao dịch cho lô lẻ(post for Odd lot)<br></br>
        /// R1 : Thỏa thuận(Repo)<br></br>
        /// AL : Tất cả Bảng giao dịch<br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20004, Order = 9)]
        public string BoardID { get; set; }

        /// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=911; required=Y; format=Int; length=16</i></para>
		/// <para><b>Tổng số lượng message thống kê</b></para>
		/// </summary>
        [JsonProperty(PropertyName = __TAG_911, Order = 10)]
        public long TotNumReports { get; set; }

        /// <summary>
        /// 2019-11-28 16:33:37 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=207; required=Y; format=String; length=2</i></para>
        /// <para><b>Mã sàn giao dịch</b></para>
        /// <para>
        /// HO: HoSE<br></br>
        /// HX: HNX
        /// </para>
		/// </summary>
        [JsonProperty(PropertyName = __TAG_207, Order = 11)]
        public string SecurityExchange { get; set; }

        /// <summary>
        /// 2019-11-28 16:33:37 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=55; required=Y; format=String; length=12</i></para>
        /// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa.</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_55, Order = 12)]
        public string Symbol { get; set; }

        /// <summary>
        /// 2019-11-28 16:40:22 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30624; required=Y; format=String; length=20</i></para>
        /// <para><b>Mã chứng khoán, (trading code, local code, stock code, bond code và v.v ...)</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30624, Order = 13)]
        public string TickerCode { get; set; }

        /// <summary>
        /// 2019-11-28 16:41:33 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30628; required=Y; format=String; length=9</i></para>
        /// <para><b>Một mã ngắn duy nhất cho một mã chứng khoán được xác định tại Sở giao dịch và được sử dụng thuận tiện hơn so với mã ISIN 12 chữ số. Nó có thể được sử dụng như một phần của mã ISIN Repo</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30628, Order = 14)]
        public string SymbolShortCode { get; set; }

        /// <summary>
        /// 2019-11-28 16:47:36 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30629; required=Y; format=String; length=120</i></para>
        /// <para><b>Tên chứng khoán.</b></para>        
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30629, Order = 15)]
        public string SymbolName { get; set; }

        /// <summary>
        /// 2019-11-28 16:48:50 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30630; required=Y; format=String; length=80</i></para>
        /// <para><b>Tên tiếng Anh của chứng khoán</b></para>        
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30630, Order = 16)]
        public string SymbolEnglishName { get; set; }

        /// <summary>
        /// 2019-11-28 16:50:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20009; required=Y; format=String; length=11</i></para>
        /// <para><b>Sản phẩm là một lớp hoặc đơn vị để định nghĩa hoặc quản lý các quy định giao dịch, thanh toán và bù trừ, tạo ra các tài sản, thống kê, v.v. Nhóm chứng khoán được gán cho Mã sản phẩm bằng cách kết hợp các thông tin Sở giao dịch, thị trường, nhóm chứng khoán, tài sản cơ sở và tiền tệ</b></para>        
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20009, Order = 17)]
        public string ProductID { get; set; }

        /// <summary>
        /// 2019-11-28 16:48:50 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20003; required=Y; format=String; length=3</i></para>
        /// <para><b>ID của nhóm sản phẩm</b></para>
        /// <para>
        /// Sở giao dịch chứng khoán Tp Hồ Chí Minh<br></br>        /// STO : Product Group thị trường cổ phiếu<br></br>        /// BDO : Product Group thị trường trái phiếu<br></br>        /// RPO : Prodcut Group thị trường Repo<br></br>        /// Sở giao dịch chứng khoán Hà Nội<br></br>        /// STX : Product Group thị trường cổ phiếu<br></br>        /// UPX : Product Group thị trường UPCOM<br></br>        /// BDX : Product Group thị trường trái phiếu chính phủ<br></br>        /// FIO : Product Group thị trường phái sinh (chỉ số)<br></br>        /// FBX : Product Group thị trường trái phiếu (trái phiếu)<br></br>        /// HCX: Product Group thị trường trái phiếu doanh nghiệp<br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20003, Order = 18)]
        public string ProductGrpID { get; set; }

        /// <summary>
        /// 2019-11-28 16:51:30 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30604; required=Y; format=String; length=2</i></para>
        /// <para><b>ID của nhóm chứng khoán (Stock, Investment Trust, Futures, Option, etc.)</b></para>        
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30604, Order = 19)]
        public string SecurityGroupID { get; set; }

        /// <summary>
        /// 2019-11-28 16:53:15 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=201 ; required=N; format=String; length=1</i></para>
        /// <para><b>Mã loại quyền của chứng khoán phái sinh</b></para>
        /// <para>
        /// P = Bán <br></br>
        /// C = Mua
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_201, Order = 20)]
        public string PutOrCall { get; set; }

        /// <summary>
        /// 2019-11-28 16:55:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1194 ; required=N; format=String; length=1</i></para>
        /// <para><b>Loại thực hiện quyền của chứng khoán phái sinh</b></para>
        /// <para>        ///A: Hợp đồng theo kiếu Mỹ<br></br>        ///E: Hợp đồng theo kiểu Châu Âu<br></br>        ///B: Bermuda-style<br></br>        ///Z: khác
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_1194, Order = 21)]
        public string ExerciseStyle { get; set; }

        /// <summary>
        /// 2019-12-09 15:25:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=200 ; required=N; format=String; length=6</i></para>
        /// <para><b>Năm tháng đáo hạn (YYYYMM) </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_200 , Order = 22)]
        public string MaturityMonthYear { get; set; }

        /// <summary>
        /// 2019-12-09 15:26:12 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=541 ; required=N; format=String; length=8</i></para>
        /// <para><b>Ngày đáo hạn  YYYYMMDD </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_541, Order = 23)]
        public string MaturityDate { get; set; }

        /// <summary>
        /// 2019-12-09 15:27:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=106 ; required=N; format=String; length=10</i></para>
        /// <para><b>Mã tổ chức phát hành (Issuer ID)</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_106, Order = 24)]
        public string Issuer { get; set; }

        /// <summary>
        /// 2019-12-09 15:28:38 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=225; required=N; format=String; length=8</i></para>
        /// <para><b>Ngày phát hành chứng khoán (YYYYMMDD) </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_225, Order = 25)]
        public string IssueDate { get; set; }

        /// <summary>
        /// 2019-12-09 15:29:11 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=231; required=Y; format=Float; length=22(12.8) </i></para>
        /// <para><b>Hệ số nhân, thể hiện giá trị hợp đồng </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_231 , Order = 26)]
        public double ContractMultiplier { get; set; }  

        /// <summary>
        /// 2019-12-09 15:29:55 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=223; required=N; format=Float; length=14(7.5)  </i></para>
        /// <para><b>Lãi suất trái phiếu. Lãi suất coupon = Các khoản thanh toán lãi định kỳ/Mệnh giá trái phiếu </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_223 , Order = 27)]
        public double CouponRate { get; set; }  

        /// <summary>
        /// 2019-12-09 15:30:15 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=15; required=Y; format=String; length=3  </i></para>
        /// <para><b>Mã tiền theo ISO 4217 VND, USD, EUR </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_15, Order = 28)]
        public string Currency { get; set; }

        /// <summary>
        /// 2019-12-09 15:40:01 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20020; required=N; format=Int; length=16  </i></para>
        /// <para><b>KLCP niêm yết </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20020, Order = 29)]
        public long ListedShares { get; set; }

        /// <summary>
        /// 2019-12-09 15:45:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1149; required=Y; format=Price; length=15(9.4)  </i></para>
        /// <para><b>Giá trần </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_1149 , Order = 30)]
        public double HighLimitPrice { get; set; }  

        /// <summary>
        /// 2019-12-09 15:45:40 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1148; required=Y; format=Price; length=15(9.4)  </i></para>
        /// <para><b>Giá sàn </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_1148 , Order = 31)]
        public double LowLimitPrice { get; set; }      

        /// <summary>
        /// 2019-12-09 15:45:55 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=202; required=Y; format=Price; length=18(9.8)   </i></para>
        /// <para><b>Giá thực hiện cho sản phẩm chứng quyền. </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_202 , Order = 32)]
        public double StrikePrice { get; set; }      

        /// <summary>
        /// 2019-12-09 15:46:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=965; required=Y; format=String; length=1 </i></para>
        /// <para><b>Trạng thái cổ phiếu có bị ngừng giao dịch hay không </b></para>
        /// <para>        ///Y = Halt <br></br>        ///N = Not Halt <br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_965, Order = 33)]
        public string SecurityStatus { get; set; }

        /// <summary>
        /// 2019-12-09 15:46:55 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30631; required=Y; format=Float; length=21(12.8) </i></para>
        /// <para><b>Đơn vị khối lượng tối thiểu (hay còn gọi là lô giao dịch), được xem như khối lượng đặt lệnh hợp lệ. Nói cách khác, khối lượng đặt lệnh phải là bội số của giá trị này. </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30631 , Order = 34)]
        public double ContractSize { get; set; }  

        /// <summary>
        /// 2019-12-09 15:46:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1193; required=N; format=String; length=1 </i></para>
        /// <para><b>Phương thức thanh toán </b></para>
        /// <para>        ///C: Tiền mặt  <br></br>        ///D: Thanh toán bằng tài sản <br></br>
        ///A: Tiền mặt và tài sản  <br></br>
        ///O: None <br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_1193, Order = 35)]
        public string SettlMethod { get; set; }

        /// <summary>
        /// 2019-12-09 15:46:55 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=236; required=N; format=Float; length=13(5.6)</i></para>
        /// <para><b>Lợi suất (lợi tức) trái phiếu được đảm bảo đến kỳ đáo hạn dựa trên giá tham chiếu. </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_236 , Order = 36)]
        public double Yield { get; set; }   

        /// <summary>
        /// 2019-12-09 15:47:07 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20013; required=Y; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá tham chiếu là mức giá cơ sở để tính giá trần/giá sàn </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20013 , Order = 37)]
        public double ReferencePrice { get; set; }   

        /// <summary>
        /// 2019-12-09 15:47:36 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20014; required=N; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá xác định cho việc định giá. Biên độ giá dựa trên giá này đến lần khớp lệnh đầu tiên. Khi khớp lệnh lần đầu xảy ra, biên độ giá sẽ được xác định lại trên giá khớp đầu tiên. </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20014 , Order = 38)]
        public double EvaluationPrice { get; set; }   

        /// <summary>
        /// 2019-12-09 15:47:50 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20015 ; required=N; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá đặt cao nhất của việc định giá phiên mở cửa </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20015 , Order = 39)]
        public double HgstOrderPrice { get; set; }   

        /// <summary>
        /// 2019-12-09 15:47:55 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20016 ; required=N; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá đặt thấp nhất của việc định giá phiên mở cửa </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20016 , Order = 40)]
        public double LwstOrderPrice { get; set; }   

        /// <summary>
        /// 2019-12-09 15:48:08 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=140  ; required=Y; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá đóng cửa phiên trước</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_140 , Order = 41)]
        public double PrevClosePx { get; set; }   

        /// <summary>
        /// 2019-12-09 15:48:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20027; required=Y; format=String; length=1 </i></para>
        /// <para><b>Phương thức của giá đóng cửa: </b></para>
        /// <para>        ///1: Giá thực hiện cuối cùng  <br></br>        ///2: Giá đặt lệnh tốt nhất   <br></br>
        ///3: Không có giao dịch  <br></br>
        ///4: "Giá đặt lệnh tốt nhất đối với chứng khoán giao dịch theo phương thức Giá khớp lệnh đầu tiên"  <br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20027, Order = 42)]
        public string SymbolCloseInfoPxType { get; set; }

        /// <summary>
        /// 2019-12-09 15:48:58 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30642  ; required=N; format=String; length=8</i></para>
        /// <para><b>Ngày giao dịch đầu tiên YYYYMMDD </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30642, Order = 43)]
        public string FirstTradingDate { get; set; }

        /// <summary>
        /// 2019-12-09 15:49:06 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30511   ; required=N; format=String; length=8</i></para>
        /// <para><b>Ngày giao dịch cuối cùng YYYYMMDD  </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30511, Order = 44)]
        public string FinalTradeDate { get; set; }

        /// <summary>
        /// 2019-12-09 15:49:23 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30512    ; required=N; format=String; length=8</i></para>
        /// <para><b>Ngày thanh toán cuối cùng YYYYMMDD </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30512, Order = 45)]
        public string FinalSettleDate { get; set; }

        /// <summary>
        /// 2019-12-09 15:49:56 hungtq; 2020-08-21 08:27:16 ngocta2 update int sang string(8)
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30301     ; required=N; format=String; length=8</i></para>
        /// <para><b>Ngày niêm yết của sản phẩm phái sinh YYYYMMDD</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30301, Order = 46)]
        public string ListingDate { get; set; }

        ///// <summary>
        ///// 2019-12-09 15:50:24 hungtq
        ///// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        ///// <para><i>tag=30540 ; required=N; format=Number; length=10</i></para>
        ///// <para><b>Khối lượng của hợp đồng phái sinh chưa thực hiện </b></para>
        ///// </summary>
        //[JsonProperty(PropertyName = __TAG_30540, Order = 47)]
        //public long OpenInterestQty { get; set; }

        ///// <summary>
        ///// 2019-12-09 15:50:58 hungtq
        ///// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        ///// <para><i>tag=30573  ; required=N; format=Price ; length=18(9.8)</i></para>
        ///// <para><b>Giá thanh toán, là giá tham chiếu cho ngày mai của hợp đồng tương lai khi nó có giao dịch từ khi </b></para>
        ///// </summary>
        //[JsonProperty(PropertyName = __TAG_30573 , Order = 48)]
        //public double SettlementPrice { get; set; }   

        /// <summary>
        /// 2019-12-09 15:51:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30614 ; required=Y; format=String; length=1 </i></para>
        /// <para><b>Điều kiện để kích hoạt kết thúc ngẫu nhiên vào cuối mỗi phiên giao dịch. Trong trường hợp mã số '2' (kích hoạt có điều kiện) thì kích hoạt thực tế được thực hiện sau khi kiểm tra xem điều kiện giá có đáp ứng 'Quy tắc Ngẫu nhiên kết thúc'  </b></para>
        /// <para>        ///0: Không kích hoạt   <br></br>        ///1: Kích hoạt vô điều kiện  <br></br>
        ///2: Kích hoạt có điều kiện <br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30614, Order = 49)]
        public string RandomEndTriggeringConditionCode { get; set; }

        /// <summary>
        /// 2019-12-09 15:51:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20018  ; required=N; format=String; length=2 </i></para>
        /// <para><b>Giao dịch không hưởng quyền </b></para>
        /// <para>        ///00: Không có <br></br>        ///01: Chia cổ phiếu/Cổ phiếu thưởng <br></br>        ///02: Chia cổ tức tiền mặt. <br></br>        ///04: Phát hành quyền <br></br>        ///03: Chia cổ phiếu/Cổ phiếu thưởng + Chia cổ tức tiền mặt. <br></br>        ///05: Phát hành quyền + Chia cổ phiếu/Cổ phiếu thưởng <br></br>        ///06:  Phát hành quyền + Chia cổ tức tiền mặt <br></br>        ///07:  Phát hành quyền + Chia cổ phiếu/Cổ phiếu thưởng + Chia cổ tức <br></br>        ///08:  Trái phiếu chuyển đổi <br></br>        ///16: Chia cổ phiếu quỹ. <br></br>        ///P1: Tách cổ phiếu <br></br>        ///P2: Gộp cổ phiếu <br></br>        ///XX: V.v. <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_20018, Order = 50)]
        public string ExClassType { get; set; }

        /// <summary>
        /// 2019-12-09 15:51:26 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30625  ; required=Y; format=Float; length=15(9.4) </i></para>
        /// <para><b>Giá bình quân gia quyền </b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30625 , Order = 51)]
        public double VWAP { get; set; }   

        /// <summary>
        /// 2019-12-09 15:52:16 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30635   ; required=N; format=String; length=3</i></para>
        /// <para><b>Ký hiệu để nhận biết tình trạng quản lý mã chứng khoán khi vi phạm các quy định về công bố thống tin, giao dịch…</b></para>
        /// <para>        /// NRM: Bình thường <br></br>
        /// WID: Cảnh báo do vi phạm CBTT <br></br>
        /// WFR: Cảnh báo do vi phạm BCTC <br></br>
        /// WOV: Cảnh báo do vi phạm khác <br></br>
        /// CTR: Kiểm soát <br></br>
        ///C&R: Kiểm soát và hạn chế giao dịch <br></br>        ///RES: Hạn chế giao dịch <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30635, Order = 52)]
        public string SymbolAdminStatusCode { get; set; }

        /// <summary>
        /// 2019-12-09 15:52:56 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30636    ; required=N; format=String; length=3</i></para>
        /// <para><b> Ký hiệu để xác định loại giao dịch cho một mã chứng khoán.</b></para>
        /// <para>        ///NRM: Bình thường <br></br>        ///SNE: Cơ chế giao dịch đặc biệt đối với chứng khoán không có giao dịch trong khoảng thời gian quy định  <br></br>        ///SLS: Cơ chế giao dịch đặc biệt đối với chứng khoán sau khi bị tạm ngưng giao dịch trong khoảng thời gian quy định. <br></br>        ///NWN: Niêm yết mới với biên độ giá thông thường <br></br>        ///NWE: Niêm yết mới với biên độ giá đặc biệt <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30636, Order = 53)]
        public string SymbolTradingMethodStatusCode { get; set; }

        /// <summary>
        /// 2019-12-09 15:53:45 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30637     ; required=N; format=String; length=3</i></para>
        /// <para><b>Ký hiệu để phân loại tình trạng giao dịch như tạm ngưng, ngưng giao dịch vì một số lý do. </b></para>
        /// <para>        ///NRM: Bình thường <br></br>        ///DTL: Hủy niêm yết để chuyển sàn <br></br>        ///SUS: Tạm ngưng giao dịch <br></br>        ///TFR: Ngưng giao dịch do bị hạn chế. <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30637, Order = 54)]
        public string SymbolTradingSantionStatusCode  { get; set; }

        /// <summary>
        /// lINHnh 02/01/2025
        /// CÓ 2 KÝ TỰ
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30647)]
        public string SectorTypeCode { get; set; }

        /// <summary>
        /// LinhNH 02/01/2025
        /// có 8 ký tự dạng YYYYMMDD
        /// </summary>
        [JsonProperty(PropertyName = __TAG_30652)]
        public string RedumptionDate { get; set; }
    }
}
