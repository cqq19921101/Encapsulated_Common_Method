using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace DBHelper
{
    public class SqlDB
    {
        private SqlConnection _conn = null;
        private SqlTransaction _st = null;
        private string _connString = "";

        public SqlDB(IDbConnection dbConnection, IsolationLevel myIsolationLevel)
        {
            _conn = (SqlConnection)dbConnection;
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public SqlDB(string ConnectionString)
        {
            _connString = ConnectionString;
            _conn = GetDBConnection();
        }

        public SqlDB(string ConnectionString, IsolationLevel myIsolationLevel)
        {
            _connString = ConnectionString;
            _conn = GetDBConnection();
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public SqlDB(IsolationLevel myIsolationLevel)
        {
            _conn = GetDBConnection();
            _st = _conn.BeginTransaction(myIsolationLevel);
        }


        public void Commit()
        {
            if (_st != null)
            {
                _st.Commit();
                _st = null;
                Close();
            }
        }

        public void Rollback()
        {
            _st.Rollback();
            _st = null;
            Close();
        }

        private void Close()
        {
            string SqlStat = _conn.State.ToString();
            if (SqlStat != "Closed")
            {
                try
                {
                    _conn.Close();
                }
                catch
                {
                    throw;
                }
            }
        }

        private SqlConnection GetDBConnection()
        {
            SqlConnection conn = new SqlConnection(_connString);
            string SqlStat = conn.State.ToString();
            if (SqlStat == "Closed")
            {
                try
                {
                    conn.Open();
                }
                catch
                {
                    throw;
                }
            }
            return conn;
        }
        public DataTable Execute(string sql)
        {
            return Execute(sql, null);
        }

        public DataTable Execute(string sql, ArrayList opc)
        {
            DataTable dt = null;
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                SqlCommand cmd = new SqlCommand(sql, _conn);
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                if (null != _st)
                    da.SelectCommand.Transaction = _st;
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        SqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        cmd.Parameters.Add(mp);
                    }
                }
                dt = new DataTable("ResultTable");
                da.Fill(dt);
                da.Dispose();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            return dt;
        }

        public string ExecuteScalar(string sql, ArrayList opc)
        {
            string sRet = "";
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                SqlCommand cmd = new SqlCommand(sql, _conn);
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        SqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        cmd.Parameters.Add(mp);
                    }
                }
                if (null != _st)
                    cmd.Transaction = _st;
                Object obj = cmd.ExecuteScalar();
                sRet = (null == obj) ? "" : obj.ToString();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            return sRet;
        }

        public int ExecuteNonQuery(string sql, ArrayList opc)
        {
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                SqlCommand cmd = new SqlCommand(sql, _conn);
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        SqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        cmd.Parameters.Add(mp);
                    }
                }
                if (null != _st)
                    cmd.Transaction = _st;
                return cmd.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
        }

        public string ExecuteScalarEx(string sql, ArrayList opc)
        {
            DataTable dt = Execute(sql, opc);
            string sRet = dt.Rows.Count > 0 ? dt.Rows[0][0].ToString() : "";
            return sRet;
        }

        public string ExecuteScalarEx(string sql)
        {
            return ExecuteScalarEx(sql, null);
        }

        // Keep this method for old code compatible
        public string ExecuteProcScalar(string procName, ArrayList opc, string outputParam)
        {
            Hashtable ht = ExecuteProc(procName, opc);
            return ht[outputParam].ToString();
        }

        // call store procedure return Return DataTable
        public DataTable ExecuteProcTable(string procName, ArrayList opc)
        {
            #region
            DataSet ds = new DataSet();
            try
            {
                SqlDataAdapter DtAdapter = new SqlDataAdapter();
                DtAdapter.SelectCommand = CreateCommand(procName, opc);
                DtAdapter.Fill(ds);
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            DataTable dt = ds.Tables[0];
            return dt;
            #endregion
        }


        //call store procedure return hashtable
        public Hashtable ExecuteProc(string procName, ArrayList opc)
        {
            #region
            SqlCommand cmd;
            try
            {
                if (null == opc)
                {
                    cmd = CreateCommand(procName, null);
                }
                else
                {
                    cmd = CreateCommand(procName, opc);
                }
                cmd.Transaction = _st;
                cmd.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            Hashtable htReturn = new Hashtable();
            foreach (SqlParameter sp in cmd.Parameters)
            {
                htReturn.Add(sp.ParameterName, sp.Value.ToString().Trim());
            }
            return htReturn;
            #endregion
        }

        //call store procedure return hashtable（datatable as input parameter）
        public Hashtable ExecuteProc(string procName, ArrayList opc, DataTable dt)
        {
            #region
            SqlCommand cmd;
            try
            {
                if (null == opc)
                {
                    cmd = CreateCommand(procName, null, dt);
                }
                else
                {
                    cmd = CreateCommand(procName, opc, dt);
                }
                cmd.Transaction = _st;
                cmd.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            Hashtable htReturn = new Hashtable();
            foreach (SqlParameter sp in cmd.Parameters)
            {
                htReturn.Add(sp.ParameterName, sp.Value.ToString().Trim());
            }
            return htReturn;
            #endregion
        }

        private SqlCommand CreateCommand(string procName, ArrayList opc)
        {
            #region
            if (null == _conn)
                _conn = GetDBConnection();
            SqlCommand cmd = new SqlCommand(procName, _conn);
            cmd.CommandType = CommandType.StoredProcedure;
            try
            {


                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        SqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        mp.Size = op.Size;
                        mp.Direction = op.Direction;
                        cmd.Parameters.Add(mp);
                    }
                }

            }
            catch
            {
                throw;
            }


            return cmd;
            #endregion
        }

        private SqlCommand CreateCommand(string procName, ArrayList opc, DataTable dt)
        {
            #region
            if (null == _conn)
                _conn = GetDBConnection();
            SqlCommand cmd = new SqlCommand(procName, _conn);
            cmd.CommandType = CommandType.StoredProcedure;
            try
            {
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        SqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        mp.Size = op.Size;
                        mp.Direction = op.Direction;
                        cmd.Parameters.Add(mp);
                    }
                }

                cmd.Parameters.AddWithValue("@P_TABLE", dt);//datatable作为sp 参数传入
            }
            catch
            {
                throw;
            }

            return cmd;
            #endregion
        }

        public void BulkInsert(DataTable dt)
        {
            BulkInsert(dt, null);
        }

        public void BulkInsert(DataTable dt, string DestTableName)
        {
            if (null == _conn)
                _conn = GetDBConnection();
            using (SqlBulkCopy s = new SqlBulkCopy(_conn))
            {
                if (DestTableName == null)
                {
                    s.DestinationTableName = dt.TableName;
                }
                else
                {
                    s.DestinationTableName = DestTableName;
                }

                foreach (var column in dt.Columns)
                    s.ColumnMappings.Add(column.ToString(), column.ToString());

                s.WriteToServer(dt);
            }
        }

        /// <summary>
        /// 用datatalbe返回查詢結果(旧function兼容)
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="opc"></param>
        /// <returns></returns>
        public DataTable GetDataTable(string sql, ArrayList opc)
        {
            DataTable dt = new DataTable();
            if (opc.Count > 0)
            {
                return dt = Execute(sql, opc);
            }
            else
            {
                return dt = Execute(sql, null);
            }

        }
    }
}
