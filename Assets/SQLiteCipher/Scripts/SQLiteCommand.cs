using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;

namespace SQLiteCipher
{
    public class SQLiteCommand
    {
        internal static IntPtr NegativePointer = new IntPtr(-1);
        private readonly List<Binding> _bindings;
        private readonly SQLiteConnection _conn;

        internal SQLiteCommand(SQLiteConnection conn)
        {
            _conn = conn;
            _bindings = new List<Binding>();
            this.CommandText = "";
        }

        public string CommandText { get; set; }

        public int ExecuteNonQuery()
        {
            if (_conn.Trace) Debug.WriteLine("Executing: " + this);

            SQLite3.Result r = SQLite3.Result.OK;
            IntPtr stmt = Prepare();
            r = SQLite3.Step(stmt);
            Finalize(stmt);
            if (r == SQLite3.Result.Done) {
                int rowsAffected = SQLite3.Changes(_conn.Handle);
                return rowsAffected;
            }

            if (r == SQLite3.Result.Error) {
                string msg = SQLite3.GetErrmsg(_conn.Handle);
                throw SQLiteException.New(r, msg);
            }

            if (r == SQLite3.Result.Constraint)
                if (SQLite3.ExtendedErrCode(_conn.Handle) == SQLite3.ExtendedResult.ConstraintNotNull)
                    throw NotNullConstraintViolationException.New(r, SQLite3.GetErrmsg(_conn.Handle));

            throw SQLiteException.New(r, r.ToString());
        }

        public IEnumerable<T> ExecuteDeferredQuery<T>()
        {
            return ExecuteDeferredQuery<T>(_conn.GetMapping(typeof(T)));
        }

        public List<T> ExecuteQuery<T>()
        {
            return ExecuteDeferredQuery<T>(_conn.GetMapping(typeof(T))).ToList();
        }

        public List<T> ExecuteQuery<T>(TableMapping map)
        {
            return ExecuteDeferredQuery<T>(map).ToList();
        }

        protected virtual void OnInstanceCreated(object obj)
        {
            // Can be overridden.
        }

        public IEnumerable<T> ExecuteDeferredQuery<T>(TableMapping map)
        {
            if (_conn.Trace) Debug.WriteLine("Executing Query: " + this);

            IntPtr stmt = Prepare();
            try {
                TableMapping.Column[] cols = new TableMapping.Column[SQLite3.ColumnCount(stmt)];

                for (int i = 0; i < cols.Length; i++) {
                    string name = SQLite3.ColumnName16(stmt, i);
                    cols[i] = map.FindColumn(name);
                }

                while (SQLite3.Step(stmt) == SQLite3.Result.Row) {
                    object obj = Activator.CreateInstance(map.MappedType);
                    for (int i = 0; i < cols.Length; i++) {
                        if (cols[i] == null)
                            continue;
                        SQLite3.ColType colType = SQLite3.ColumnType(stmt, i);
                        object val = ReadCol(stmt, i, colType, cols[i].ColumnType);
                        cols[i].SetValue(obj, val);
                    }

                    OnInstanceCreated(obj);
                    yield return (T)obj;
                }
            } finally {
                SQLite3.Finalize(stmt);
            }
        }

        public T ExecuteScalar<T>()
        {
            if (_conn.Trace) Debug.WriteLine("Executing Query: " + this);

            T val = default(T);

            IntPtr stmt = Prepare();

            try {
                SQLite3.Result r = SQLite3.Step(stmt);
                if (r == SQLite3.Result.Row) {
                    SQLite3.ColType colType = SQLite3.ColumnType(stmt, 0);
                    val = (T)ReadCol(stmt, 0, colType, typeof(T));
                } else if (r == SQLite3.Result.Done) { } else {
                    throw SQLiteException.New(r, SQLite3.GetErrmsg(_conn.Handle));
                }
            } finally {
                Finalize(stmt);
            }

            return val;
        }

        public void Bind(string name, object val)
        {
            _bindings.Add(new Binding {
                Name = name,
                Value = val
            });
        }

        public void Bind(object val)
        {
            Bind(null, val);
        }

        public override string ToString()
        {
            string[] parts = new string[1 + _bindings.Count];
            parts[0] = this.CommandText;
            int i = 1;
            foreach (Binding b in _bindings) {
                parts[i] = string.Format("  {0}: {1}", i - 1, b.Value);
                i++;
            }

            return string.Join(Environment.NewLine, parts);
        }

        private Sqlite3Statement Prepare()
        {
            IntPtr stmt = SQLite3.Prepare2(_conn.Handle, this.CommandText);
            BindAll(stmt);
            return stmt;
        }

        private void Finalize(Sqlite3Statement stmt)
        {
            SQLite3.Finalize(stmt);
        }

        private void BindAll(Sqlite3Statement stmt)
        {
            int nextIdx = 1;
            foreach (Binding b in _bindings) {
                if (b.Name != null)
                    b.Index = SQLite3.BindParameterIndex(stmt, b.Name);
                else
                    b.Index = nextIdx++;

                BindParameter(stmt, b.Index, b.Value, _conn.StoreDateTimeAsTicks);
            }
        }

        internal static int BindParameter(Sqlite3Statement stmt, int index, object value, bool storeDateTimeAsTicks)
        {
            if (value == null)
                return SQLite3.BindNull(stmt, index);
            if (value is int)
                return SQLite3.BindInt(stmt, index, (int)value);
            if (value is string)
                return SQLite3.BindText(stmt, index, (string)value, -1, NegativePointer);
            if (value is byte || value is ushort || value is sbyte || value is short)
                return SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
            if (value is bool)
                return SQLite3.BindInt(stmt, index, (bool)value ? 1 : 0);
            if (value is uint || value is long)
                return SQLite3.BindInt64(stmt, index, Convert.ToInt64(value));
            if (value is float || value is double || value is decimal)
                return SQLite3.BindDouble(stmt, index, Convert.ToDouble(value));
            if (value is TimeSpan)
                return SQLite3.BindInt64(stmt, index, ((TimeSpan)value).Ticks);
            if (value is DateTime) {
                if (storeDateTimeAsTicks)
                    return SQLite3.BindInt64(stmt, index, ((DateTime)value).Ticks);
                return SQLite3.BindText(stmt, index, ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), -1,
                    NegativePointer);
            }

            if (value is DateTimeOffset)
                return SQLite3.BindInt64(stmt, index, ((DateTimeOffset)value).UtcTicks);

            {
                if (value.GetType().IsEnum)
                    return SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
            }

            if (value is byte[])
                return SQLite3.BindBlob(stmt, index, (byte[])value, ((byte[])value).Length, NegativePointer);
            if (value is Guid)
                return SQLite3.BindText(stmt, index, ((Guid)value).ToString(), 72, NegativePointer);

            throw new NotSupportedException("Cannot store type: " + value.GetType());
        }

        private object ReadCol(Sqlite3Statement stmt, int index, SQLite3.ColType type, Type clrType)
        {
            if (type == SQLite3.ColType.Null)
                return null;

            if (clrType == typeof(string)) return SQLite3.ColumnString(stmt, index);

            if (clrType == typeof(int)) return SQLite3.ColumnInt(stmt, index);

            if (clrType == typeof(bool)) return SQLite3.ColumnInt(stmt, index) == 1;

            if (clrType == typeof(double)) return SQLite3.ColumnDouble(stmt, index);

            if (clrType == typeof(float)) return (float)SQLite3.ColumnDouble(stmt, index);

            if (clrType == typeof(TimeSpan)) return new TimeSpan(SQLite3.ColumnInt64(stmt, index));

            if (clrType == typeof(DateTime)) {
                if (_conn.StoreDateTimeAsTicks) return new DateTime(SQLite3.ColumnInt64(stmt, index));

                string text = SQLite3.ColumnString(stmt, index);
                return DateTime.Parse(text);
            }

            if (clrType == typeof(DateTimeOffset)) {
                return new DateTimeOffset(SQLite3.ColumnInt64(stmt, index), TimeSpan.Zero);
            }

            if (clrType.IsEnum) {
                return SQLite3.ColumnInt(stmt, index);
            }

            if (clrType == typeof(long)) return SQLite3.ColumnInt64(stmt, index);

            if (clrType == typeof(uint)) return (uint)SQLite3.ColumnInt64(stmt, index);

            if (clrType == typeof(decimal)) return (decimal)SQLite3.ColumnDouble(stmt, index);

            if (clrType == typeof(byte)) return (byte)SQLite3.ColumnInt(stmt, index);

            if (clrType == typeof(ushort)) return (ushort)SQLite3.ColumnInt(stmt, index);

            if (clrType == typeof(short)) return (short)SQLite3.ColumnInt(stmt, index);

            if (clrType == typeof(sbyte)) return (sbyte)SQLite3.ColumnInt(stmt, index);

            if (clrType == typeof(byte[])) return SQLite3.ColumnByteArray(stmt, index);

            if (clrType == typeof(Guid)) {
                string text = SQLite3.ColumnString(stmt, index);
                return new Guid(text);
            }

            throw new NotSupportedException("Don't know how to read " + clrType);
        }

        private class Binding
        {
            public string Name { get; set; }
            public object Value { get; set; }
            public int Index { get; set; }
        }
    }
}