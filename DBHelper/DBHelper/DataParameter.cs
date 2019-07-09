using System;
using System.Data;
using System.Xml;
using System.Configuration;
using LiteOn.Corp.IT.EA.CMCC;
namespace DBHelper
{
    public class DataPara
    {
        public string ParameterName;
        public DbType Type;
        public object Value;
        public ParameterDirection Direction;
        public Int32 Size;
        public static DataPara CreateDataParameter(string ParameterName, DbType type, object Value, ParameterDirection Direction)
        {
            DataPara dp = new DataPara();
            dp.ParameterName = ParameterName;
            dp.Type = type;
            dp.Value = Value;
            dp.Direction = Direction;
            return dp;
        }

        /// <summary>
        /// Create SQL Commend Parameter
        /// </summary>
        /// <param name="ParameterName"></param>
        /// <param name="type"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static DataPara CreateDataParameter(string ParameterName, DbType type, object Value)
        {
            return CreateDataParameter(ParameterName, type, Value, ParameterDirection.Input);
        }

        /// <summary>
        /// Create SQL Procedure Parameter
        /// </summary>
        /// <param name="ParameterName"></param>
        /// <param name="type"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static DataPara CreateDataParameter(string ParameterName, DbType type, object Value, ParameterDirection Direction, Int32 Size)
        {
            DataPara dp = new DataPara();
            dp.ParameterName = ParameterName;
            dp.Type = type;
            dp.Size = Size;
            dp.Direction = Direction;
            dp.Value = Value;
            return dp;
        }

        public static string GetConnectionString(string key)
        {
            // THIS WILL RETURN "" IF NO CORRECT SETTING FOUND
            var appSettings = ConfigurationManager.AppSettings;
            var conSettings = ConfigurationManager.ConnectionStrings;
            string s = appSettings[key] ?? "";
            if (s == "")
            {
                try
                {
                    s = conSettings[key].ConnectionString ?? "";
                }
                catch { }
            }
            return s;
        }

        /// <summary>
        /// Default is load from "DBConnectionString"
        /// </summary>
        /// <returns></returns>
        public static string GetConnectionString()
        {
            return GetConnectionString("DBConnectionString");
        }

        public static string GetDbConnectionString(string DBType)
        {
            AccInfoClass AccInfo = new AccInfoClass();
            CMCCAccessClass CMCCAccess = new CMCCAccessClass();
            AccInfo = CMCCAccess.Access(DBType);

            XmlDocument xDoc = new XmlDocument();
            xDoc.Load(@"C:\Projects\Config\DBConfig.xml");
            string DBSour = xDoc.DocumentElement.SelectSingleNode(DBType + "DBServer").InnerText.Trim();
            string DBName = xDoc.DocumentElement.SelectSingleNode(DBType + "DBName").InnerText.Trim();

            string connString = FormatConnStr(DBSour, DBName, AccInfo.Account, AccInfo.Password);
            return connString;
        }

        public static string FormatConnStr(string Server, string Database, string ID, string pwd)
        {
            string strConn = string.Empty;
            string ConnStringFormat = "Data Source={0};Initial Catalog={1};User ID={2};Password={3}";
            strConn = string.Format(ConnStringFormat, Server, Database, ID, pwd);
            return strConn;
        }
    }
}
