using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;

namespace SQLiteCipher
{
    public abstract class BaseTableQuery
    {
        protected class Ordering
        {
            public string ColumnName { get; set; }
            public bool Ascending { get; set; }
        }
    }

    public class TableQuery<T> : BaseTableQuery, IEnumerable<T>
    {
        private bool bDeferred;
        private BaseTableQuery JoinInner;
        private Expression JoinInnerKeySelector;
        private BaseTableQuery JoinOuter;
        private Expression JoinOuterKeySelector;
        private Expression JoinSelector;
        private int? Limit;
        private int? Offset;
        private List<Ordering> OrderBys;
        private Expression SelectorExp;
        private Expression WhereExp;

        public SQLiteConnection Connection { get; private set; }
        public TableMapping Table { get; private set; }

        private TableQuery(SQLiteConnection conn, TableMapping table)
        {
            this.Connection = conn;
            this.Table = table;
        }

        public TableQuery(SQLiteConnection conn)
        {
            this.Connection = conn;
            this.Table = this.Connection.GetMapping(typeof(T));
        }

        public IEnumerator<T> GetEnumerator()
        {
            UnityEngine.Profiling.Profiler.BeginSample("ExecuteQuery");
            IEnumerator<T> it;
            if (!bDeferred) {
                it = GenerateCommand("*").ExecuteQuery<T>().GetEnumerator();
            } else {
                it = GenerateCommand("*").ExecuteDeferredQuery<T>().GetEnumerator();
            }
            UnityEngine.Profiling.Profiler.EndSample();
            return it;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public TableQuery<U> Clone<U>()
        {
            TableQuery<U> q = new TableQuery<U>(this.Connection, this.Table);
            q.WhereExp = WhereExp;
            q.bDeferred = bDeferred;
            if (OrderBys != null) q.OrderBys = new List<Ordering>(OrderBys);
            q.Limit = Limit;
            q.Offset = Offset;
            q.JoinInner = JoinInner;
            q.JoinInnerKeySelector = JoinInnerKeySelector;
            q.JoinOuter = JoinOuter;
            q.JoinOuterKeySelector = JoinOuterKeySelector;
            q.JoinSelector = JoinSelector;
            q.SelectorExp = SelectorExp;
            return q;
        }

        public TableQuery<T> Where(Expression<Func<T, bool>> predExpr)
        {
            if (predExpr.NodeType == ExpressionType.Lambda) {
                LambdaExpression lambda = predExpr;
                Expression pred = lambda.Body;
                AddWhere(pred);
                return this;
            }

            throw new NotSupportedException("Must be a predicate");
        }

        public TableQuery<T> Take(int n)
        {
            Limit = n;
            return this;
        }

        public TableQuery<T> Skip(int n)
        {
            Offset = n;
            return this;
        }

        public TableQuery<T> NextTake()
        {
            if (null != Offset) {
                Offset += Limit;
            } else {
                Offset = Limit;
            }
            return this;
        }

        public T ElementAt(int index)
        {
            return Skip(index).Take(1).First();
        }

        public TableQuery<T> Deferred()
        {
            bDeferred = true;
            return this;
        }

        public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        public TableQuery<T> ThenBy<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, true);
        }

        public TableQuery<T> ThenByDescending<U>(Expression<Func<T, U>> orderExpr)
        {
            return AddOrderBy(orderExpr, false);
        }

        private TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpr, bool asc)
        {
            if (orderExpr.NodeType == ExpressionType.Lambda) {
                LambdaExpression lambda = orderExpr;

                MemberExpression mem = null;

                UnaryExpression unary = lambda.Body as UnaryExpression;
                if (unary != null && unary.NodeType == ExpressionType.Convert)
                    mem = unary.Operand as MemberExpression;
                else
                    mem = lambda.Body as MemberExpression;

                if (mem != null && mem.Expression.NodeType == ExpressionType.Parameter) {

                    if (OrderBys == null) OrderBys = new List<Ordering>();
                    OrderBys.Add(new Ordering {
                        ColumnName = this.Table.FindColumnWithPropertyName(mem.Member.Name).Name,
                        Ascending = asc
                    });
                    return this;
                }

                throw new NotSupportedException("Order By does not support: " + orderExpr);
            }

            throw new NotSupportedException("Must be a predicate");
        }

        private void AddWhere(Expression pred)
        {
            if (WhereExp == null)
                WhereExp = pred;
            else
                WhereExp = Expression.AndAlso(WhereExp, pred);
        }

        public TableQuery<TResult> Join<TInner, TKey, TResult>(
            TableQuery<TInner> inner,
            Expression<Func<T, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<T, TInner, TResult>> resultSelector)
        {
            TableQuery<TResult> q =
                new TableQuery<TResult>(this.Connection, this.Connection.GetMapping(typeof(TResult))) {
                    JoinOuter = this,
                    JoinOuterKeySelector = outerKeySelector,
                    JoinInner = inner,
                    JoinInnerKeySelector = innerKeySelector,
                    JoinSelector = resultSelector
                };
            return q;
        }

        public TableQuery<TResult> Select<TResult>(Expression<Func<T, TResult>> selector)
        {
            TableQuery<TResult> q = Clone<TResult>();
            q.SelectorExp = selector;
            return q;
        }

        private SQLiteCommand GenerateCommand(string selectionList)
        {
            if (JoinInner != null && JoinOuter != null)
                throw new NotSupportedException("Joins are not supported.");

            string cmdText = "select " + selectionList + " from \"" + this.Table.TableName + "\"";
            List<object> args = new List<object>();
            if (WhereExp != null) {
                CompileResult w = CompileExpr(WhereExp, args);
                cmdText += " where " + w.CommandText;
            }

            if (OrderBys != null && OrderBys.Count > 0) {
                string t = string.Join(", ",
                    OrderBys.Select(o => "\"" + o.ColumnName + "\"" + (o.Ascending ? "" : " desc")).ToArray());
                cmdText += " order by " + t;
            }

            if (Limit.HasValue) cmdText += " limit " + Limit.Value;
            if (Offset.HasValue) {
                if (!Limit.HasValue) cmdText += " limit -1 ";
                cmdText += " offset " + Offset.Value;
            }

            return this.Connection.CreateCommand(cmdText, args.ToArray());
        }

        private CompileResult CompileExpr(Expression expr, List<object> queryArgs)
        {
            if (expr == null)
                throw new NotSupportedException("Expression is NULL");

            if (expr is BinaryExpression) {
                BinaryExpression bin = (BinaryExpression)expr;

                CompileResult leftr = CompileExpr(bin.Left, queryArgs);
                CompileResult rightr = CompileExpr(bin.Right, queryArgs);

                //If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
                string text;
                if (leftr.CommandText == "?" && leftr.Value == null)
                    text = CompileNullBinaryExpression(bin, rightr);
                else if (rightr.CommandText == "?" && rightr.Value == null)
                    text = CompileNullBinaryExpression(bin, leftr);
                else
                    text = "(" + leftr.CommandText + " " + GetSqlName(bin) + " " + rightr.CommandText + ")";
                return new CompileResult { CommandText = text };
            }

            if (expr.NodeType == ExpressionType.Call) {
                MethodCallExpression call = (MethodCallExpression)expr;
                CompileResult[] args = new CompileResult[call.Arguments.Count];
                CompileResult obj = call.Object != null ? CompileExpr(call.Object, queryArgs) : null;

                for (int i = 0; i < args.Length; i++) args[i] = CompileExpr(call.Arguments[i], queryArgs);

                string sqlCall = "";

                if (call.Method.Name == "Like" && args.Length == 2) {
                    sqlCall = "(" + args[0].CommandText + " like " + args[1].CommandText + ")";
                } else if (call.Method.Name == "Contains" && args.Length == 2) {
                    sqlCall = "(" + args[1].CommandText + " in " + args[0].CommandText + ")";
                } else if (call.Method.Name == "Contains" && args.Length == 1) {
                    if (call.Object != null && call.Object.Type == typeof(string))
                        sqlCall = "(" + obj.CommandText + " like ('%' || " + args[0].CommandText + " || '%'))";
                    else
                        sqlCall = "(" + args[0].CommandText + " in " + obj.CommandText + ")";
                } else if (call.Method.Name == "StartsWith" && args.Length == 1) {
                    sqlCall = "(" + obj.CommandText + " like (" + args[0].CommandText + " || '%'))";
                } else if (call.Method.Name == "EndsWith" && args.Length == 1) {
                    sqlCall = "(" + obj.CommandText + " like ('%' || " + args[0].CommandText + "))";
                } else if (call.Method.Name == "Equals" && args.Length == 1) {
                    sqlCall = "(" + obj.CommandText + " = (" + args[0].CommandText + "))";
                } else if (call.Method.Name == "ToLower") {
                    sqlCall = "(lower(" + obj.CommandText + "))";
                } else if (call.Method.Name == "ToUpper") {
                    sqlCall = "(upper(" + obj.CommandText + "))";
                } else {
                    sqlCall = call.Method.Name.ToLower() + "(" +
                              string.Join(",", args.Select(a => a.CommandText).ToArray()) + ")";
                }

                return new CompileResult { CommandText = sqlCall };
            }

            if (expr.NodeType == ExpressionType.Constant) {
                ConstantExpression c = (ConstantExpression)expr;
                queryArgs.Add(c.Value);
                return new CompileResult {
                    CommandText = "?",
                    Value = c.Value
                };
            }

            if (expr.NodeType == ExpressionType.Convert) {
                UnaryExpression u = (UnaryExpression)expr;
                Type ty = u.Type;
                CompileResult valr = CompileExpr(u.Operand, queryArgs);
                return new CompileResult {
                    CommandText = valr.CommandText,
                    Value = valr.Value != null ? ConvertTo(valr.Value, ty) : null
                };
            }

            if (expr.NodeType == ExpressionType.MemberAccess) {
                MemberExpression mem = (MemberExpression)expr;

                if (mem.Expression != null && mem.Expression.NodeType == ExpressionType.Parameter) {
                    //
                    // This is a column of our table, output just the column name
                    // Need to translate it if that column name is mapped
                    //
                    string columnName = this.Table.FindColumnWithPropertyName(mem.Member.Name).Name;
                    return new CompileResult { CommandText = "\"" + columnName + "\"" };
                }

                object obj = null;
                if (mem.Expression != null) {
                    CompileResult r = CompileExpr(mem.Expression, queryArgs);
                    if (r.Value == null) throw new NotSupportedException("Member access failed to compile expression");
                    if (r.CommandText == "?") queryArgs.RemoveAt(queryArgs.Count - 1);
                    obj = r.Value;
                }

                //
                // Get the member value
                //
                object val = null;

                if (mem.Member.MemberType == MemberTypes.Property) {
                    PropertyInfo m = (PropertyInfo)mem.Member;
                    val = m.GetValue(obj, null);
                } else if (mem.Member.MemberType == MemberTypes.Field) {
                    FieldInfo m = (FieldInfo)mem.Member;
                    val = m.GetValue(obj);
                } else {
                    throw new NotSupportedException("MemberExpr: " + mem.Member.MemberType);
                }

                //
                // Work special magic for enumerables
                //
                if (val != null && val is IEnumerable && !(val is string) && !(val is IEnumerable<byte>)) {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("(");
                    string head = "";
                    foreach (object a in (IEnumerable)val) {
                        queryArgs.Add(a);
                        sb.Append(head);
                        sb.Append("?");
                        head = ",";
                    }

                    sb.Append(")");
                    return new CompileResult {
                        CommandText = sb.ToString(),
                        Value = val
                    };
                }

                queryArgs.Add(val);
                return new CompileResult {
                    CommandText = "?",
                    Value = val
                };
            }

            throw new NotSupportedException("Cannot compile: " + expr.NodeType);
        }

        private static object ConvertTo(object obj, Type t)
        {
            if (obj == null)
                return null;

            Type nut = Nullable.GetUnderlyingType(t);
            if (nut == null)
                return Convert.ChangeType(obj, t);
            return Convert.ChangeType(obj, nut);
        }

        private string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter)
        {
            switch (expression.NodeType) {
                case ExpressionType.Equal:
                    return "(" + parameter.CommandText + " is ?)";
                case ExpressionType.NotEqual:
                    return "(" + parameter.CommandText + " is not ?)";
                default:
                    throw new NotSupportedException("Cannot compile Null-BinaryExpression with type " +
                                                    expression.NodeType);
            }
        }

        private string GetSqlName(Expression expr)
        {
            ExpressionType n = expr.NodeType;

            switch (n) {
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.And:
                    return "&";
                case ExpressionType.AndAlso:
                    return "and";
                case ExpressionType.Or:
                    return "|";
                case ExpressionType.OrElse:
                    return "or";
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                default:
                    throw new NotSupportedException("Cannot get SQL for: " + n);
            }
        }

        public int Count()
        {
            return GenerateCommand("count(*)").ExecuteScalar<int>();
        }

        public int Count(Expression<Func<T, bool>> predExpr)
        {
            return Where(predExpr).Count();
        }

        public T First()
        {
            TableQuery<T> query = Take(1);
            return query.ToList().First();
        }

        public T FirstOrDefault()
        {
            TableQuery<T> query = Take(1);
            return query.ToList().FirstOrDefault();
        }

        private class CompileResult
        {
            public string CommandText { get; set; }
            public object Value { get; set; }
        }
    }
}