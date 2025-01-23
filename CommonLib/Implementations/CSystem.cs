using CommonLib.Interfaces;
using System;
using System.Diagnostics;
using System.Management;


namespace CommonLib.Implementations
{
    public class CSystem: ISystem
    {
        /// <summary>
        /// lay ten username run app
        /// </summary>
        /// <param name="processName">notepad.exe</param>
        /// <returns>FIT-NGOCTA2\ngocta2</returns>
        public string GetProcessOwner(string processName)
        {
            string query = "Select * from Win32_Process Where Name = \"" + processName + "\"";
            ManagementObjectSearcher searcher = new ManagementObjectSearcher(query);
            ManagementObjectCollection processList = searcher.Get();

            foreach (ManagementObject obj in processList)
            {
                string[] argList = new string[] { string.Empty, string.Empty };
                int returnVal = Convert.ToInt32(obj.InvokeMethod("GetOwner", argList));
                if (returnVal == 0)
                {
                    // return DOMAIN\user
                    string owner = argList[1] + "\\" + argList[0];
                    return owner;
                }
            }

            return "NO OWNER";
        }

        public static string GetCaller(int level = 2)
        {
            var m = new StackTrace().GetFrame(level).GetMethod();

            if (m.DeclaringType == null) return ""; //9:33 AM 6/18/2014 Exception Details: System.NullReferenceException: Object reference not set to an instance of an object.

            // .Name is the name only, .FullName includes the namespace
            var className = m.DeclaringType.FullName;

            //the method/function name you are looking for.
            var methodName = m.Name;

            //returns a composite of the namespace, class and method name.
            return className + "->" + methodName;
        }

        /// <summary>
        /// ke thua tu 5G
        /// 
        /// 3=>0 = Microsoft.Samples.AspNetRouteIntegration.Service->HelloWorld=>BaseLib.CSQL->ExecuteSP=>BaseLib.CBase->GetDeepCaller=>BaseLib.CBase->GetCaller=>
        /// 3=>2 = Microsoft.Samples.AspNetRouteIntegration.Service->HelloWorld=>BaseLib.CSQL->ExecuteSP=>
        /// </summary>
        /// <returns></returns>
        public static string GetCallStack()
        {
            string strCallerName = "";
            for (int i = 3; i >= 2; i--)
                strCallerName += GetCaller(i) + "=>";

            //returns a composite of the namespace, class and method name.
            return strCallerName;
        }
    }
}
