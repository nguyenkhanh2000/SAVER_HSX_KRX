using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-24 14:25:42 ngocta2
    /// ghi lai dau vet thong tin code dc exec o dau, luc nao
    /// </summary>
    public interface ITrace
    {
        /// <summary>
        /// prop: full datetime cua thoi diem xay ra event
        /// </summary>
        string Timestamp { get; set; }

        /// <summary>
        /// prop: thread id cua thread exec code (thread id trong 1 process)
        /// </summary>
        int ThreadId { get; set; }

        /// <summary>
        /// string thread id
        /// </summary>
        string ThreadIdS { get; set; }

        /// <summary>
        /// prop: task id cua task exec code (task id trong 1 process)
        /// </summary>
        int TaskId { get; set; }

        /// <summary>
        /// string task id 
        /// </summary>
        string TaskIdS { get; set; }

        /// <summary>
        /// lay ten cua function hien tai
        /// </summary>
        string FunctionName { get; set; }
    }
}
