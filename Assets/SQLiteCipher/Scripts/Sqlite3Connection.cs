using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using SQLite.Attribute;
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;

namespace SQLiteCipher
{
    public class SQLiteConnection : IDisposable
    {
        internal static readonly Sqlite3DatabaseHandle NullHandle = default(Sqlite3DatabaseHandle);

        public Sqlite3DatabaseHandle Handle { get; private set; }
        public string DatabasePath { get; private set; }
        public bool TimeExecution { get; set; }
        public bool Trace { get; set; }
        public bool StoreDateTimeAsTicks { get; private set; }

        private readonly Random Rand = new Random();

        private TimeSpan busyTimeout;
        private long ElapsedMilliseconds;
        private Dictionary<string, TableMapping> Mappings;
        private bool Opened;
        private Stopwatch StopWatch;
        private Dictionary<string, TableMapping> Tables;

        private int TransactionDepth;

        public TimeSpan BusyTimeout {
            get { return busyTimeout; }
            set {
                busyTimeout = value;
                if (Handle != NullHandle)
                    SQLite3.BusyTimeout(Handle, (int)busyTimeout.TotalMilliseconds);
            }
        }

        public IEnumerable<TableMapping> TableMappings {
            get { return Tables != null ? Tables.Values : Enumerable.Empty<TableMapping>(); }
        }

        public bool IsInTransaction {
            get { return TransactionDepth > 0; }
        }

        public SQLiteConnection(string databasePath, string password = null, bool storeDateTimeAsTicks = false) : this(
            databasePath, password, SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create, storeDateTimeAsTicks)
        { }

        public SQLiteConnection(string databasePath, string password, SQLiteOpenFlags openFlags,
            bool storeDateTimeAsTicks = false)
        {
            if (string.IsNullOrEmpty(databasePath))
                throw new ArgumentException("Must be specified", "databasePath");

            DatabasePath = databasePath;

            Sqlite3DatabaseHandle handle;

            byte[] databasePathAsBytes = GetNullTerminatedUtf8(this.DatabasePath);
            SQLite3.Result r = SQLite3.Open(databasePathAsBytes, out handle, (int)openFlags, IntPtr.Zero);

            Handle = handle;
            if (r != SQLite3.Result.OK)
                throw SQLiteException.New(r,
                    string.Format("Could not open database file: {0} ({1})", this.DatabasePath, r));

            if (!string.IsNullOrEmpty(password)) {
                SQLite3.Result result = SQLite3.Key(handle, password, password.Length);
                if (result != SQLite3.Result.OK)
                    throw SQLiteException.New(r,
                        string.Format("Could not open database file: {0} ({1})", this.DatabasePath, r));
            }

            Opened = true;

            StoreDateTimeAsTicks = storeDateTimeAsTicks;
            BusyTimeout = TimeSpan.FromSeconds(0.1);
        }

        ~SQLiteConnection()
        {
            Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (Opened && NullHandle != Handle) {
                try {
                    if (null != Mappings) {
                        foreach (var item in Mappings.Values) {
                            item.Dispose();
                        }
                    }
                    SQLite3.Result r = SQLite3.Close(Handle);
                    if (r != SQLite3.Result.OK) {
                        string msg = SQLite3.GetErrmsg(Handle);
                        throw SQLiteException.New(r, msg);
                    }
                } finally {
                    Handle = NullHandle;
                    Opened = false;
                }

            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void EnableLoadExtension(int onoff)
        {
            SQLite3.Result r = SQLite3.EnableLoadExtension(this.Handle, onoff);
            if (r != SQLite3.Result.OK) {
                string msg = SQLite3.GetErrmsg(this.Handle);
                throw SQLiteException.New(r, msg);
            }
        }

        private static byte[] GetNullTerminatedUtf8(string s)
        {
            int utf8Length = Encoding.UTF8.GetByteCount(s);
            byte[] bytes = new byte[utf8Length + 1];
            utf8Length = Encoding.UTF8.GetBytes(s, 0, s.Length, bytes, 0);
            return bytes;
        }

        public TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            if (Mappings == null) Mappings = new Dictionary<string, TableMapping>();
            TableMapping map;
            if (!Mappings.TryGetValue(type.FullName, out map)) {
                map = new TableMapping(type, createFlags);
                Mappings[type.FullName] = map;
            }

            return map;
        }

        public TableMapping GetMapping<T>()
        {
            return GetMapping(typeof(T));
        }

        public int DropTable<T>()
        {
            TableMapping map = GetMapping(typeof(T));

            string query = string.Format("drop table if exists \"{0}\"", map.TableName);

            return Execute(query);
        }

        public int DropTable(Type t)
        {
            TableMapping map = GetMapping(t);

            string query = string.Format("drop table if exists \"{0}\"", map.TableName);

            return Execute(query);
        }

        public int CreateTable<T>(CreateFlags createFlags = CreateFlags.None)
        {
            return CreateTable(typeof(T), createFlags);
        }

        public int CreateTable(Type ty, CreateFlags createFlags = CreateFlags.None)
        {
            if (Tables == null) Tables = new Dictionary<string, TableMapping>();
            TableMapping map;
            if (!Tables.TryGetValue(ty.FullName, out map)) {
                map = GetMapping(ty, createFlags);
                Tables.Add(ty.FullName, map);
            }

            string query = "create table if not exists \"" + map.TableName + "\"(\n";

            IEnumerable<string> decls = map.Columns.Select(p => Orm.SqlDecl(p, this.StoreDateTimeAsTicks));
            string decl = string.Join(",\n", decls.ToArray());
            query += decl;
            query += ")";

            int count = Execute(query);

            if (count == 0) MigrateTable(map);

            Dictionary<string, IndexInfo> indexes = new Dictionary<string, IndexInfo>();
            foreach (TableMapping.Column c in map.Columns)
                foreach (IndexedAttribute i in c.Indices) {
                    string iname = i.Name ?? map.TableName + "_" + c.Name;
                    IndexInfo iinfo;
                    if (!indexes.TryGetValue(iname, out iinfo)) {
                        iinfo = new IndexInfo {
                            IndexName = iname,
                            TableName = map.TableName,
                            Unique = i.Unique,
                            Columns = new List<IndexedColumn>()
                        };
                        indexes.Add(iname, iinfo);
                    }

                    if (i.Unique != iinfo.Unique)
                        throw new Exception(
                            "All the columns in an index must have the same value for their Unique property");

                    iinfo.Columns.Add(new IndexedColumn {
                        Order = i.Order,
                        ColumnName = c.Name
                    });
                }

            foreach (string indexName in indexes.Keys) {
                IndexInfo index = indexes[indexName];
                string[] columns = index.Columns.OrderBy(i => i.Order).Select(i => i.ColumnName).ToArray();
                count += CreateIndex(indexName, index.TableName, columns, index.Unique);
            }

            return count;
        }

        public int CreateIndex(string indexName, string tableName, string[] columnNames, bool unique = false)
        {
            const string sqlFormat = "create {2} index if not exists \"{3}\" on \"{0}\"(\"{1}\")";
            string sql = string.Format(sqlFormat, tableName, string.Join("\", \"", columnNames), unique ? "unique" : "",
                indexName);
            return Execute(sql);
        }

        public int CreateIndex(string indexName, string tableName, string columnName, bool unique = false)
        {
            return CreateIndex(indexName, tableName, new[] { columnName }, unique);
        }

        public int CreateIndex(string tableName, string columnName, bool unique = false)
        {
            return CreateIndex(tableName + "_" + columnName, tableName, columnName, unique);
        }

        public int CreateIndex(string tableName, string[] columnNames, bool unique = false)
        {
            return CreateIndex(tableName + "_" + string.Join("_", columnNames), tableName, columnNames, unique);
        }

        public void CreateIndex<T>(Expression<Func<T, object>> property, bool unique = false)
        {
            MemberExpression mx;
            if (property.Body.NodeType == ExpressionType.Convert)
                mx = ((UnaryExpression)property.Body).Operand as MemberExpression;
            else
                mx = property.Body as MemberExpression;
            PropertyInfo propertyInfo = mx.Member as PropertyInfo;
            if (propertyInfo == null)
                throw new ArgumentException("The lambda expression 'property' should point to a valid Property");

            string propName = propertyInfo.Name;

            TableMapping map = GetMapping<T>();
            string colName = map.FindColumnWithPropertyName(propName).Name;

            CreateIndex(map.TableName, colName, unique);
        }

        public List<ColumnInfo> GetTableInfo(string tableName)
        {
            string query = "pragma table_info(\"" + tableName + "\")";
            return Query<ColumnInfo>(query);
        }

        private void MigrateTable(TableMapping map)
        {
            List<ColumnInfo> existingCols = GetTableInfo(map.TableName);

            List<TableMapping.Column> toBeAdded = new List<TableMapping.Column>();

            foreach (TableMapping.Column p in map.Columns) {
                bool found = false;
                foreach (ColumnInfo c in existingCols) {
                    found = string.Compare(p.Name, c.Name, StringComparison.OrdinalIgnoreCase) == 0;
                    if (found)
                        break;
                }

                if (!found) toBeAdded.Add(p);
            }

            foreach (TableMapping.Column p in toBeAdded) {
                string addCol = "alter table \"" + map.TableName + "\" add column " +
                                Orm.SqlDecl(p, this.StoreDateTimeAsTicks);
                Execute(addCol);
            }
        }

        protected virtual SQLiteCommand NewCommand()
        {
            return new SQLiteCommand(this);
        }

        public SQLiteCommand CreateCommand(string cmdText, params object[] ps)
        {
            if (!Opened)
                throw SQLiteException.New(SQLite3.Result.Error, "Cannot create commands from unopened database");

            SQLiteCommand cmd = NewCommand();
            cmd.CommandText = cmdText;
            foreach (object o in ps) cmd.Bind(o);
            return cmd;
        }

        public int Execute(string query, params object[] args)
        {
            SQLiteCommand cmd = CreateCommand(query, args);

            if (this.TimeExecution) {
                if (StopWatch == null) StopWatch = new Stopwatch();
                StopWatch.Reset();
                StopWatch.Start();
            }

            int r = cmd.ExecuteNonQuery();

            if (this.TimeExecution) {
                StopWatch.Stop();
                ElapsedMilliseconds += StopWatch.ElapsedMilliseconds;
                Debug.WriteLine("Finished in {0} ms ({1:0.0} s total)", StopWatch.ElapsedMilliseconds,
                    ElapsedMilliseconds / 1000.0);
            }

            return r;
        }

        public T ExecuteScalar<T>(string query, params object[] args)
        {
            SQLiteCommand cmd = CreateCommand(query, args);

            if (this.TimeExecution) {
                if (StopWatch == null) StopWatch = new Stopwatch();
                StopWatch.Reset();
                StopWatch.Start();
            }

            T r = cmd.ExecuteScalar<T>();

            if (this.TimeExecution) {
                StopWatch.Stop();
                ElapsedMilliseconds += StopWatch.ElapsedMilliseconds;
                Debug.WriteLine("Finished in {0} ms ({1:0.0} s total)", StopWatch.ElapsedMilliseconds,
                    ElapsedMilliseconds / 1000.0);
            }

            return r;
        }

        public List<T> Query<T>(string query, params object[] args) where T : new()
        {
            SQLiteCommand cmd = CreateCommand(query, args);
            return cmd.ExecuteQuery<T>();
        }

        public IEnumerable<T> DeferredQuery<T>(string query, params object[] args) where T : new()
        {
            SQLiteCommand cmd = CreateCommand(query, args);
            return cmd.ExecuteDeferredQuery<T>();
        }

        public List<object> Query(TableMapping map, string query, params object[] args)
        {
            SQLiteCommand cmd = CreateCommand(query, args);
            return cmd.ExecuteQuery<object>(map);
        }

        public IEnumerable<object> DeferredQuery(TableMapping map, string query, params object[] args)
        {
            SQLiteCommand cmd = CreateCommand(query, args);
            return cmd.ExecuteDeferredQuery<object>(map);
        }

        public TableQuery<T> Table<T>() where T : new()
        {
            return new TableQuery<T>(this);
        }

        public T Get<T>(object pk) where T : new()
        {
            TableMapping map = GetMapping(typeof(T));
            return Query<T>(map.GetByPrimaryKeySQL, pk).First();
        }

        public T Get<T>(Expression<Func<T, bool>> predicate) where T : new()
        {
            return Table<T>().Where(predicate).First();
        }

        public T Find<T>(object pk) where T : new()
        {
            TableMapping map = GetMapping(typeof(T));
            return Query<T>(map.GetByPrimaryKeySQL, pk).FirstOrDefault();
        }

        public object Find(object pk, TableMapping map)
        {
            return Query(map, map.GetByPrimaryKeySQL, pk).FirstOrDefault();
        }

        public T Find<T>(Expression<Func<T, bool>> predicate) where T : new()
        {
            return Table<T>().Where(predicate).FirstOrDefault();
        }

        public void BeginTransaction()
        {
            if (Interlocked.CompareExchange(ref TransactionDepth, 1, 0) == 0)
                try {
                    Execute("begin transaction");
                } catch (Exception ex) {
                    SQLiteException sqlExp = ex as SQLiteException;
                    if (sqlExp != null)
                        switch (sqlExp.Result) {
                            case SQLite3.Result.IOError:
                            case SQLite3.Result.Full:
                            case SQLite3.Result.Busy:
                            case SQLite3.Result.NoMem:
                            case SQLite3.Result.Interrupt:
                                RollbackTo(null, true);
                                break;
                        } else
                        Interlocked.Decrement(ref TransactionDepth);

                    throw;
                }
            else
                throw new InvalidOperationException("Cannot begin a transaction while already in a transaction.");
        }

        public string SaveTransactionPoint()
        {
            int depth = Interlocked.Increment(ref TransactionDepth) - 1;
            string retVal = "S" + Rand.Next(short.MaxValue) + "D" + depth;

            try {
                Execute("savepoint " + retVal);
            } catch (Exception ex) {
                SQLiteException sqlExp = ex as SQLiteException;
                if (sqlExp != null)
                    switch (sqlExp.Result) {
                        case SQLite3.Result.IOError:
                        case SQLite3.Result.Full:
                        case SQLite3.Result.Busy:
                        case SQLite3.Result.NoMem:
                        case SQLite3.Result.Interrupt:
                            RollbackTo(null, true);
                            break;
                    } else
                    Interlocked.Decrement(ref TransactionDepth);

                throw;
            }

            return retVal;
        }

        public void Rollback()
        {
            RollbackTo(null, false);
        }

        public void RollbackTo(string savepoint)
        {
            RollbackTo(savepoint, false);
        }

        private void RollbackTo(string savepoint, bool noThrow)
        {
            try {
                if (string.IsNullOrEmpty(savepoint)) {
                    if (Interlocked.Exchange(ref TransactionDepth, 0) > 0) Execute("rollback");
                } else {
                    DoSavePointExecute(savepoint, "rollback to ");
                }
            } catch (SQLiteException) {
                if (!noThrow)
                    throw;
            }
        }

        public void Release(string savepoint)
        {
            DoSavePointExecute(savepoint, "release ");
        }

        private void DoSavePointExecute(string savepoint, string cmd)
        {
            int firstLen = savepoint.IndexOf('D');
            if (firstLen >= 2 && savepoint.Length > firstLen + 1) {
                int depth;
                if (int.TryParse(savepoint.Substring(firstLen + 1), out depth))
                    if (0 <= depth && depth < TransactionDepth) {
#if NETFX_CORE
						Volatile.Write (ref _transactionDepth, depth);
#elif SILVERLIGHT
						_transactionDepth = depth;
#else
                        Thread.VolatileWrite(ref TransactionDepth, depth);
#endif
                        Execute(cmd + savepoint);
                        return;
                    }
            }

            throw new ArgumentException(
                "savePoint is not valid, and should be the result of a call to SaveTransactionPoint.", "savePoint");
        }

        public void Commit()
        {
            if (Interlocked.Exchange(ref TransactionDepth, 0) != 0) Execute("commit");
        }

        public void RunInTransaction(Action action)
        {
            try {
                string savePoint = SaveTransactionPoint();
                action();
                Release(savePoint);
            } catch (Exception) {
                Rollback();
                throw;
            }
        }

        public int InsertAll(IEnumerable objects)
        {
            int c = 0;
            RunInTransaction(() => {
                foreach (object r in objects) c += Insert(r);
            });
            return c;
        }

        public int InsertAll(IEnumerable objects, string extra)
        {
            int c = 0;
            RunInTransaction(() => {
                foreach (object r in objects) c += Insert(r, extra);
            });
            return c;
        }

        public int InsertAll(IEnumerable objects, Type objType)
        {
            int c = 0;
            RunInTransaction(() => {
                foreach (object r in objects) c += Insert(r, objType);
            });
            return c;
        }

        public int Insert(object obj)
        {
            if (obj == null) return 0;
            return Insert(obj, "", obj.GetType());
        }

        public int InsertOrReplace(object obj)
        {
            if (obj == null) return 0;
            return Insert(obj, "OR REPLACE", obj.GetType());
        }

        public int Insert(object obj, Type objType)
        {
            return Insert(obj, "", objType);
        }

        public int InsertOrReplace(object obj, Type objType)
        {
            return Insert(obj, "OR REPLACE", objType);
        }

        public int Insert(object obj, string extra)
        {
            if (obj == null) return 0;
            return Insert(obj, extra, obj.GetType());
        }

        public int Insert(object obj, string extra, Type objType)
        {
            if (obj == null || objType == null) return 0;

            TableMapping map = GetMapping(objType);

            if (map.PK != null && map.PK.IsAutoGuid) {
                PropertyInfo prop = objType.GetProperty(map.PK.PropertyName);
                if (prop != null)
                    if (prop.GetValue(obj, null).Equals(Guid.Empty))
                        prop.SetValue(obj, Guid.NewGuid(), null);
            }

            bool replacing = string.Compare(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;

            TableMapping.Column[] cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;
            object[] vals = new object[cols.Length];
            for (int i = 0; i < vals.Length; i++) vals[i] = cols[i].GetValue(obj);

            PreparedSqliteInsertCommand insertCmd = map.GetInsertCommand(this, extra);
            int count;

            try {
                count = insertCmd.ExecuteNonQuery(vals);
            } catch (SQLiteException ex) {
                if (SQLite3.ExtendedErrCode(this.Handle) == SQLite3.ExtendedResult.ConstraintNotNull)
                    throw NotNullConstraintViolationException.New(ex.Result, ex.Message, map, obj);
                throw;
            }

            if (map.HasAutoIncPK) {
                long id = SQLite3.LastInsertRowid(this.Handle);
                map.SetAutoIncPK(obj, id);
            }

            return count;
        }

        public int Update(object obj)
        {
            if (obj == null) return 0;
            return Update(obj, obj.GetType());
        }

        public int Update(object obj, Type objType)
        {
            int rowsAffected = 0;
            if (obj == null || objType == null) return 0;

            TableMapping map = GetMapping(objType);
            TableMapping.Column pk = map.PK;

            if (pk == null) throw new NotSupportedException("Cannot update " + map.TableName + ": it has no PK");

            IEnumerable<TableMapping.Column> cols = from p in map.Columns where p != pk select p;
            IEnumerable<object> vals = from c in cols select c.GetValue(obj);
            List<object> ps = new List<object>(vals);
            ps.Add(pk.GetValue(obj));

            string q = string.Format("update \"{0}\" set {1} where {2} = ? ", map.TableName,
                string.Join(",", (from c in cols select "\"" + c.Name + "\" = ? ").ToArray()), pk.Name);

            try {
                rowsAffected = Execute(q, ps.ToArray());
            } catch (SQLiteException ex) {
                if (ex.Result == SQLite3.Result.Constraint &&
                    SQLite3.ExtendedErrCode(this.Handle) == SQLite3.ExtendedResult.ConstraintNotNull)
                    throw NotNullConstraintViolationException.New(ex, map, obj);

                throw ex;
            }

            return rowsAffected;
        }

        public int UpdateAll(IEnumerable objects)
        {
            int c = 0;
            RunInTransaction(() => {
                foreach (object r in objects) c += Update(r);
            });
            return c;
        }

        public int Delete(object objectToDelete)
        {
            TableMapping map = GetMapping(objectToDelete.GetType());
            TableMapping.Column pk = map.PK;
            if (pk == null) throw new NotSupportedException("Cannot delete " + map.TableName + ": it has no PK");
            string q = string.Format("delete from \"{0}\" where \"{1}\" = ?", map.TableName, pk.Name);
            return Execute(q, pk.GetValue(objectToDelete));
        }

        public int Delete<T>(object primaryKey)
        {
            TableMapping map = GetMapping(typeof(T));
            TableMapping.Column pk = map.PK;
            if (pk == null) throw new NotSupportedException("Cannot delete " + map.TableName + ": it has no PK");
            string q = string.Format("delete from \"{0}\" where \"{1}\" = ?", map.TableName, pk.Name);
            return Execute(q, primaryKey);
        }

        public int DeleteAll<T>()
        {
            TableMapping map = GetMapping(typeof(T));
            string query = string.Format("delete from \"{0}\"", map.TableName);
            return Execute(query);
        }

        public void Close()
        {
            if (Opened && this.Handle != NullHandle)
                try {
                    if (Mappings != null)
                        foreach (TableMapping sqlInsertCommand in Mappings.Values)
                            sqlInsertCommand.Dispose();
                    SQLite3.Result r = SQLite3.Close(this.Handle);
                    if (r != SQLite3.Result.OK) {
                        string msg = SQLite3.GetErrmsg(this.Handle);
                        throw SQLiteException.New(r, msg);
                    }
                } finally {
                    this.Handle = NullHandle;
                    Opened = false;
                }
        }



        private struct IndexedColumn
        {
            public int Order;
            public string ColumnName;
        }

        private struct IndexInfo
        {
            public string IndexName;
            public string TableName;
            public bool Unique;
            public List<IndexedColumn> Columns;
        }

        public class ColumnInfo
        {
            //			public int cid { get; set; }

            [Column("name")] public string Name { get; set; }

            //			[Column ("type")]
            //			public string ColumnType { get; set; }

            public int notnull { get; set; }

            //			public string dflt_value { get; set; }

            //			public int pk { get; set; }

            public override string ToString()
            {
                return this.Name;
            }
        }
    }

    internal class SQLiteConnectionString
    {
        public string ConnectionString { get; private set; }
        public string DatabasePath { get; private set; }
        public bool StoreDateTimeAsTicks { get; private set; }

        public SQLiteConnectionString(string databasePath, bool storeDateTimeAsTicks)
        {
            this.ConnectionString = databasePath;
            this.StoreDateTimeAsTicks = storeDateTimeAsTicks;
            this.DatabasePath = databasePath;
        }
    }

}