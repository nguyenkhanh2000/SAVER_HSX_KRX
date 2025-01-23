using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
    public class EIndexs
    {
        /// <summary>
        /// <para>szie: 3</para>
        /// <para>type: Mod-96</para>
        /// </summary>
        //[DataMember(Name = __SHORT_CONFIRM_NUMBER)]
        //public long ConfirmNumber { get; set; }

        public string STAT_ControlCode { get; set; }
        public string STAT_Time { get; set; }
        public string STAT_Date { get; set; }

        //VNINDEX 2021-05-28 hungtq2
        //public string VNINDEX_ChangePercent{ get; set; }
        //public string VNINDEX_Change{ get; set; }
        //public string VNINDEX_Index{ get; set; }          // 43092     - VNIndex (Long, 4bytes)
        //public string VNINDEX_TotalTrade{ get; set; }     //  - số lượng giao dịch (long, 4bytes)
        //public string VNINDEX_TotalShares{ get; set; }      //   - Tổng khối lượng (Double, 8bytes)
        //public string VNINDEX_TotalValues{ get; set; }     //    - Tổng giá trị (Double, 8bytes)
        //public string VNINDEX_UpVolume{ get; set; }                //  Tồng KL của các CK tăng giá(Double, 8bytes) …
        //public string VNINDEX_DownVolume{ get; set; }              //  Tồng KL của các CK giảm giá(Double, 8bytes) …
        //public string VNINDEX_NoChangeVolume{ get; set; }          // Tổng KL của các CK đứng giá (Double, 8bytes)
        //public string VNINDEX_Up{ get; set; }                // Short 2 Tổng số CK tăng giá …
        //public string VNINDEX_Down{ get; set; }              // Short 2 Tổng số CK giảm giá …
        //public string VNINDEX_NoChange{ get; set; }          // Short 2 Tổng số CK đứng giá …
        //public string VNINDEX_Time{ get; set; }                // Thời gian của máy chủ giao dịch (Long, 4bytes)

        //TOTALMKT, VNINDEX
        public string VNI_ChangePercent { get; set; }
        public string VNI_Change { get; set; }
        public string VNI_IndexValue { get; set; }      // 1. IndexValue
        public string VNI_TotalTrade { get; set; }
        public string VNI_TotalSharesAOM { get; set; }  // 2. TotalSharesAOM
        public string VNI_TotalValuesAOM { get; set; }  // 3. TotalValuesAOM
        public string VNI_UpVolume { get; set; }
        public string VNI_NoChangeVolume { get; set; }
        public string VNI_DownVolume { get; set; }
        public string VNI_Up { get; set; }     // 4. Up
        public string VNI_Down { get; set; }     // 5. Down
        public string VNI_NoChange { get; set; }
        public string VNI_Time { get; set; }
        public string VNI_Ceiling { get; set; }  // so ma tang tran (tu tinh)
        public string VNI_Floor { get; set; }    // so ma giam san (tu tinh)
        public string VNI_TotalSharesOld { get; set; } // luu so cu de tinh ra data chart (vol trong tung phut)

        //VN30
        public string VN30_ChangePercent { get; set; }
        public string VN30_Change { get; set; }
        public string VN30_IndexValue { get; set; }
        public string VN30_TotalSharesAOM { get; set; }
        public string VN30_TotalValuesAOM { get; set; }
        public string VN30_TotalSharesPT { get; set; }
        public string VN30_TotalValuesPT { get; set; }
        public string VN30_Up { get; set; }
        public string VN30_Down { get; set; }
        public string VN30_NoChange { get; set; }
        public string VN30_Ceiling { get; set; }
        public string VN30_Floor { get; set; }
        public string VN30_Time { get; set; }



        //public string VN30_TotalSharesAOMOld{ get; set; }// luu so cu de tinh ra data chart (vol trong tung phut)

        //VN100
        public string VN100_ChangePercent { get; set; }
        public string VN100_Change { get; set; }
        public string VN100_IndexValue { get; set; }
        public string VN100_TotalSharesAOM { get; set; }
        public string VN100_TotalValuesAOM { get; set; }
        public string VN100_TotalSharesPT { get; set; }
        public string VN100_TotalValuesPT { get; set; }
        public string VN100_Up { get; set; }
        public string VN100_Down { get; set; }
        public string VN100_NoChange { get; set; }
        public string VN100_Ceiling { get; set; }
        public string VN100_Floor { get; set; }
        public string VN100_Time { get; set; }


        //public string VN100_TotalSharesAOMOld{ get; set; }// luu so cu de tinh ra data chart (vol trong tung phut)

        //VNALL
        public string VNALL_ChangePercent { get; set; }
        public string VNALL_Change { get; set; }
        public string VNALL_IndexValue { get; set; }
        public string VNALL_TotalSharesAOM { get; set; }
        public string VNALL_TotalValuesAOM { get; set; }
        public string VNALL_TotalSharesPT { get; set; }
        public string VNALL_TotalValuesPT { get; set; }
        public string VNALL_Up { get; set; }
        public string VNALL_Down { get; set; }
        public string VNALL_NoChange { get; set; }
        public string VNALL_Ceiling { get; set; }
        public string VNALL_Floor { get; set; }
        public string VNALL_Time { get; set; }

        //VNXALL            {2016-10-26 14:28:36 ngocta2}
        public string VNXALL_ChangePercent { get; set; }
        public string VNXALL_Change { get; set; }
        public string VNXALL_IndexValue { get; set; }
        public string VNXALL_TotalSharesAOM { get; set; }
        public string VNXALL_TotalValuesAOM { get; set; }
        public string VNXALL_TotalSharesPT { get; set; }
        public string VNXALL_TotalValuesPT { get; set; }
        public string VNXALL_Up { get; set; }
        public string VNXALL_Down { get; set; }
        public string VNXALL_NoChange { get; set; }
        public string VNXALL_Ceiling { get; set; }
        public string VNXALL_Floor { get; set; }
        public string VNXALL_Time { get; set; }

        //public string VNALL_TotalSharesAOMOld{ get; set; }// luu so cu de tinh ra data chart (vol trong tung phut)

        //VNMID
        public string VNMID_ChangePercent { get; set; }
        public string VNMID_Change { get; set; }
        public string VNMID_IndexValue { get; set; }
        public string VNMID_TotalSharesAOM { get; set; }
        public string VNMID_TotalValuesAOM { get; set; }
        public string VNMID_TotalSharesPT { get; set; }
        public string VNMID_TotalValuesPT { get; set; }
        public string VNMID_Up { get; set; }
        public string VNMID_Down { get; set; }
        public string VNMID_NoChange { get; set; }
        public string VNMID_Ceiling { get; set; }
        public string VNMID_Floor { get; set; }
        public string VNMID_Time { get; set; }


        //public string VNMID_TotalSharesAOMOld{ get; set; }// luu so cu de tinh ra data chart (vol trong tung phut)

        //VNSML
        public string VNSML_ChangePercent { get; set; }
        public string VNSML_Change { get; set; }
        public string VNSML_IndexValue { get; set; }
        public string VNSML_TotalSharesAOM { get; set; }
        public string VNSML_TotalValuesAOM { get; set; }
        public string VNSML_TotalSharesPT { get; set; }
        public string VNSML_TotalValuesPT { get; set; }
        public string VNSML_Up { get; set; }
        public string VNSML_Down { get; set; }
        public string VNSML_NoChange { get; set; }
        public string VNSML_Ceiling { get; set; }
        public string VNSML_Floor { get; set; }
        public string VNSML_Time { get; set; }

        //public string VNSML_TotalSharesAOMOld{ get; set; }// luu so cu de tinh ra data chart (vol trong tung phut)

        //iNAV
        public string INAV_StockNo { get; set; }
        public string INAV_StockSymbol { get; set; }
        public string INAV_iNAV { get; set; }
        public string INAV_Time { get; set; }

        //iIndex
        public string IINDEX_iIndexSymbol { get; set; }
        public string IINDEX_ETFSymbol { get; set; }
        public string IINDEX_IndexSymbol { get; set; }
        public string IINDEX_iIndex { get; set; }
        public string IINDEX_Time { get; set; }
    }
}
