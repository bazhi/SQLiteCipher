using System;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System.Threading;

namespace example
{
    public class CreateDBScript : MonoBehaviour
    {
        [SerializeField]
        public Text DebugText;

        private DataService DS = new DataService("TestDatabase.db");
        private bool SearchDB = false;
        private float SearchBeginTime = 0;
        List<Person> List = new List<Person>();

        private void Start()
        {
            DS.Connect();
        }

        private void Update()
        {
            if (Input.GetKey(KeyCode.C)) {
                Clear();
                DS.CreateDB();
                ToConsole("Create DB");
            } else if (Input.GetKey(KeyCode.I)) {
                Clear();
                DS.InsertInfo();
                ToConsole("Insert Info");
            } else if (Input.GetKey(KeyCode.R)) {
                Clear();
                DS.Connect();
                ToConsole("Connect");
            } else if (Input.GetKey(KeyCode.D)) {
                Clear();
                DS.Disconnect();
                ToConsole("Disconnect");
            } else if (Input.GetKeyDown(KeyCode.S)) {
                if (!SearchDB) {
                    Clear("");
                    SearchBeginTime = Time.realtimeSinceStartup;
                    SearchDB = true;
                    StartCoroutine(ShowInfo());
                }
            } else {
                if (List.Count > 0) {
                    Clear("Searching:" + List.Count);
                }
            }

        }

        private System.Collections.IEnumerator ShowInfo()
        {
            List.Clear();
            yield return StartCoroutine(DS.GetPersons(0, 50, List));
            ToConsole(List);
            SearchDB = false;
            List.Clear();
            GC.Collect();
        }

        private void Clear(string text = "")
        {
            DebugText.text = text;
        }

        private void ToConsole(List<Person> people)
        {
            ToConsole("Count:" + people.Count + " used time:" + (Time.realtimeSinceStartup - SearchBeginTime));
        }

        private void ToConsole(string msg)
        {
            if (DebugText.text.Length > 0) {
                DebugText.text += Environment.NewLine;
            }
            DebugText.text += msg;
        }
    }
}
