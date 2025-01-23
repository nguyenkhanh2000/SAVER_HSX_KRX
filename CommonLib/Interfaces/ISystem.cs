using System;
using System.Collections.Generic;
using System.Text;

namespace CommonLib.Interfaces
{
    public interface ISystem
    {
        string GetProcessOwner(string processName);
        //string GetCallStack();
    }
}
