using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HSX.Entities
{
	/// <summary>
	/// struct chua data cua file LS
	/// 
	/// 2019-08-28 13:11:50 ngocta2 update
	///	đọc LS chỉ để lấy Open chính xác vì LE hay bị mất data dẫn đến Open ko đúng
	///+ ghi REDIS key HISTORY => phải có Open chuẩn của cả phiên
	///+ pub data realtime HISTORY => phải có Open chuẩn của cả phiên
	///vay nen ko can dung struct & logic phuc tap o day
	/// </summary>
	public class ELS
	{
		//public int ConfirmNo { get; set; }
		//public int StockNo { get; set; }
		
		public long MatchedVol { get; set; }
		public double Price { get; set; }
		public string Side  { get; set; }

		/// <summary>
		/// 2019-08-28 11:17:40 ngocta2
		/// khong chuan neu dung server time cua FPTS, vi neu bat lai luc 10h, doc lai data tu 9h, lay luon time 10h
		/// su dung cach map voi data ben LE. VD: day la data cua ma ABT (Stockno=3)
		/// data vi du nay mo phong LE bi thieu data giua time 91655 va 92000 
		/// data LS van du data giua time 91655 va 92000 nhung do LE mat data nen ko map dc de lay time
		/// 
		/// /// LE
		/// Stockno		AccumulatedVol	Time
		/// 3			100				91508
		/// 3			500				91600
		/// 3			2500			91655
		/// 3			15500			92000
		/// 
		/// /// LS
		/// Stockno		MatchedVol		mapping		AccVol	No
		/// 3			100				=> 91508	100		1
		/// 3			400				=> 91600	500		2
		/// 3			2000			=> 91655	2500	3
		/// 3			3000			=> ???		5500	4
		/// 3			5000			=> ???		10500	5
		/// 3			5000			=> 92000	15500	6
		/// 
		/// tao dic luu thong tin tong vol va time tuong ung. 
		/// khi read file LE xong thi add ngay element vao dic leTimeDic
		/// leTimeDic["3_100"]		= "91508"
		/// leTimeDic["3_500"]		= "91600"
		/// leTimeDic["3_2500"]		= "91655"
		/// leTimeDic["3_15500"]	= "92000"
		/// 
		/// khi read file LS xong thi add ngay element vao dic lsDic
		/// lsDic["ABT"] = List<ELS>
		/// moi ELS la 1 obj gom {StockNo,MatchedVol,Price,Side,SelfCalculate,AccVol,Open,Highest,Lowest}
		/// StockNo,MatchedVol,Price,Side lay tu data trong file LS => SelfCalculate=0
		/// SelfCalculate,AccVol la data tu tinh							=> SelfCalculate=1
		/// 
		/// NEU lsDic["ABT"] chua co List<ELS> nao thi tao 1 list List<ELS> ban dau
		/// List<ELS> ban dau nay se co them 1 element ELS voi SelfCalculate=1 va AccVol=0
		/// 
		/// NEU lsDic["ABT"] da co List<ELS> => chac chan da co 1 element ELS voi SelfCalculate=1
		/// add dan cac element co No=1,2,3,4,5,6 vao List<ELS>, cu the nhu sau:
		/// voi No=1 thi 
		///		buoc 0: xoa tat ca element trong List thoa ma dieu kien element.SelfCalculate=0 => neu debug thi bo qua buoc nay => vay thi list nay luon chi co 2 elements
		///		buoc 1: tinh AccVol = cach lay AccVol cua element co SelfCalculate=1 (0) + MatchedVol (100) => 100
		///		buoc 2: update AccVol=100 cua element co SelfCalculate=1
		///		buoc 3: tinh ra key theo cong thuc StockNo + "_" + AccVol => "3_100"
		///		buoc 4: lay ra time mapping voi ben LE bang cach truy xuat vao leTimeDic["3_100"] lay ra "91508"
		///		buoc 5: xoa element => leTimeDic["3_100"]
		///		buoc 6: tao element ELS co cac data tu file LS va data tu tinh o buoc 4 (Time => "91508")
		///		buoc 7: them element ELS vao list
		///		buoc 8: insert redis data detail => BO QUA
		/// </summary>
		public string Time { get; set; }

		/// <summary>
		/// data tu tinh
		/// 0 - neu la data cua So tra ve
		/// 1 - neu la data tu tinh toan
		/// </summary>
		public int SelfCalculate { get; set; } = 0;

		/// <summary>
		/// data tu tinh
		/// tong khoi luong cong don
		/// </summary>
		public int AccVol { get; set; } = 0;

		/// <summary>
		/// data tu tinh
		/// = LS.Price cua row dau tien
		/// chi update vao element co SelfCalculate=1 , chinh la ele dau tien trong list
		/// </summary>
		public double Open { get; set; }

		/// <summary>
		/// data tu tinh
		/// = LE.Highest
		/// chi update vao element co SelfCalculate=1 , chinh la ele dau tien trong list
		/// </summary>
		public double Highest { get; set; }

		/// <summary>
		/// data tu tinh
		/// = LE.Lowest
		/// chi update vao element co SelfCalculate=1 , chinh la ele dau tien trong list
		/// </summary>
		public double Lowest { get; set; }
	}
}
