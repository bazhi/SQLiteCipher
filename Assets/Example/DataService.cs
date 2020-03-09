using System;
using System.Collections.Generic;
using SQLiteCipher;
using UnityEngine;

#if !UNITY_EDITOR
using System.Collections;
using System.IO;
#endif
namespace example
{
    public class DataService
    {
        private SQLiteConnection Connection;
        private string Password;
        private string DbPath;

        public DataService(string DatabaseName, string password = null)
        {
            Password = password;
#if UNITY_EDITOR
            DbPath = string.Format(@"Assets/StreamingAssets/{0}", DatabaseName);
#else

            DbPath = string.Format("{0}/{1}", Application.persistentDataPath, DatabaseName);
#endif
        }

        public void Connect()
        {
            if (null == Connection) {
                Connection = new SQLiteConnection(DbPath, Password);
            }
        }

        public void Disconnect()
        {
            if (null != Connection) {
                Connection.Dispose();
                Connection = null;
            }
        }

        public void CreateDB()
        {
            if (null == Connection) {
                return;
            }
            Connection.DropTable<Person>();
            Connection.CreateTable<Person>();
        }

        public void InsertInfo()
        {
            if (null == Connection) {
                return;
            }
            Connection.InsertAll(new[]
            {
                    new Person
                    {
                        Id = 1,
                        Name = "Tom",
                        Surname = "Perez" + Time.realtimeSinceStartup,
                        Age = 56
                    },
                    new Person
                    {
                        Id = 2,
                        Name = "Fred",
                        Surname = "Arthurson" + Time.realtimeSinceStartup,
                        Age = 16
                    },
                    new Person
                    {
                        Id = 3,
                        Name = "John",
                        Surname = "Doe" + Time.realtimeSinceStartup,
                        Age = 25
                    },
                    new Person
                    {
                        Id = 4,
                        Name = "Roberto",
                        Surname = "Huertas" + Time.realtimeSinceStartup,
                        Age = 37
                    }
                });

        }

        public System.Collections.IEnumerator GetPersons(int max, int step, List<Person> list)
        {
            if (null != Connection) {
                var query = Connection.Table<Person>();
                var it = query.Take(step).GetEnumerator();
                while (true) {
                    yield return null;
                    int count = list.Count;
                    while (it.MoveNext()) {
                        list.Add(it.Current);
                    }
                    if (max > 0) {
                        max -= step;
                        if (max <= 0) {
                            break;
                        }
                    }
                    if (list.Count < count + 10) {
                        break;
                    }
                    it = query.NextTake().GetEnumerator();
                }
            }
            yield return null;
        }

        public IEnumerable<Person> GetPersonsNamedRoberto()
        {
            if (null == Connection) {
                return default(IEnumerable<Person>);
            }
            return Connection.Table<Person>().Where(x => x.Name == "Roberto");
        }

        public Person GetJohnny()
        {
            if (null == Connection) {
                return null;
            }
            return Connection.Table<Person>().Where(x => x.Name == "Johnny").FirstOrDefault();
        }

        public Person CreatePerson()
        {
            if (null == Connection) {
                return null;
            }
            Person p = new Person {
                Name = "Johnny",
                Surname = "Mnemonic",
                Age = 21
            };
            Connection.Insert(p);
            return p;
        }
    }
}