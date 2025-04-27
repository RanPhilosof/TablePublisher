using Microsoft.VisualStudio.TestPlatform.Utilities;
using System.Runtime.CompilerServices;
using System.Xml.Linq;
using Xunit.Abstractions;
using static PublisherXUnitTests.StateOfTheArtTablePublisherTest;

namespace PublisherXUnitTests
{
    public class StateOfTheArtTablePublisherTest
    {
        private readonly ITestOutputHelper _output;

        public StateOfTheArtTablePublisherTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void AutoResetEventTest1()
        {
            var semaphoreSlim = new SemaphoreSlim(0);

            var manualResetEvent = new ManualResetEvent(false);

            Task.Run(() => { manualResetEvent.Set(); manualResetEvent.Set(); _output.WriteLine("Signaled Twiced"); });

            Thread.Sleep(5000);
            _output.WriteLine("WaitOne");
            manualResetEvent.WaitOne();

            Thread.Sleep(1000);
            _output.WriteLine("WaitOne");
            manualResetEvent.WaitOne();
            manualResetEvent.Reset();


            _output.WriteLine("Passed");

            manualResetEvent.WaitOne(100);
            _output.WriteLine("Finished");

        }

        [Fact]

        public void PublisherTest1()
        {

        }

        [Fact]
        public void TriggerManagerTest1()
        {
            triggers.Clear();
            triggersTime.Clear();

            var methodTrigger = new MethodTrigger(25, 500);
            methodTrigger.FireTrigger += MethodTrigger_FireTrigger;

            _output.WriteLine($"Triggering 10 Times: {DateTime.Now.TimeOfDay.ToString()}");

            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update(); ;
            Thread.Sleep(1_000);
            _output.WriteLine("1. Slept 1 sec");
            methodTrigger.Update();
            Thread.Sleep(1_000);
            _output.WriteLine("2. Slept 1 sec");

            _output.WriteLine($"Triggering 5 Times: {DateTime.Now.TimeOfDay.ToString()}");

            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            _output.WriteLine("Sleeping 1 sec");
            Thread.Sleep(1_000);

            Assert.Equal(triggers.Count, 3.0, 0.0);

            triggers.Clear();
            triggersTime.Clear();
        }

        [Fact]
        public void TriggerManagerTest2()
        {
            triggers.Clear();
            triggersTime.Clear();

            var methodTrigger = new MethodTrigger(25, 500);
            methodTrigger.FireTrigger += MethodTrigger_FireTrigger;

            _output.WriteLine($"Triggering 10 Tasks The Triggers 1 Million Times Each: {DateTime.Now.TimeOfDay.ToString()}");

            Task.Run(
                () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        Task.Factory.StartNew(
                            () =>
                            {
                                for (int j = 0; j < 1_000_000;)
                                    methodTrigger.Update();
                            },
                            TaskCreationOptions.LongRunning);
                    }
                });

            Thread.Sleep(10_000);

            Assert.Equal(triggers.Count, 20.0, 2.0);

            List<double> deltas = triggersTime.Zip(triggersTime.Skip(1), (prev, next) => (next - prev).TotalSeconds).ToList();
            var avgDelta = deltas.Average();
            var stdDev = Math.Sqrt(deltas.Average(d => Math.Pow(d - avgDelta, 2)));

            _output.WriteLine($"Avg Delta Time: {avgDelta} seconds, Std: {stdDev} seconds, Min: {deltas.Min()} Max: {deltas.Max()}");

            triggers.Clear();
            triggersTime.Clear();
        }

        [Fact]
        public void TriggerManagerTest3()
        {
            triggers.Clear();
            triggersTime.Clear();
            insertedNewRecordTriggersTime.Clear();
            beforeInsertedNewRecordTriggersTime.Clear();

            var methodTrigger = new MethodTrigger(25, 500);
            methodTrigger.FireTrigger += MethodTrigger_FireTrigger;

            _output.WriteLine($"Triggering 11 Times: {DateTime.Now.TimeOfDay.ToString()}");

            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            beforeInsertedNewRecordTriggersTime.Add(DateTime.Now);
            methodTrigger.Inserted();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            beforeInsertedNewRecordTriggersTime.Add(DateTime.Now);
            methodTrigger.Inserted();
            Thread.Sleep(10);
            beforeInsertedNewRecordTriggersTime.Add(DateTime.Now);
            methodTrigger.Inserted();
            Thread.Sleep(10);
            beforeInsertedNewRecordTriggersTime.Add(DateTime.Now);
            methodTrigger.Inserted();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(5_000);
            _output.WriteLine("Slept 5 sec");

            // Should Get 1 Updates
            // Should Get 4 Inserted

            _output.WriteLine($"triggers count: {triggers.Count}, suppose to be 5");

            Assert.Equal(5, triggers.Count, 0.5);

            List<double> deltas = beforeInsertedNewRecordTriggersTime.Zip(insertedNewRecordTriggersTime, (prev, next) => (next - prev).TotalSeconds).ToList();
            var avgDelta = deltas.Average();
            var stdDev = Math.Sqrt(deltas.Average(d => Math.Pow(d - avgDelta, 2)));

            _output.WriteLine($"Avg Delta Time: {avgDelta} seconds, Std: {stdDev} seconds, Min: {deltas.Min()} Max: {deltas.Max()}");


            triggers.Clear();
            triggersTime.Clear();
            insertedNewRecordTriggersTime.Clear();
            beforeInsertedNewRecordTriggersTime.Clear();

            _output.WriteLine($"Triggering 4 Times: {DateTime.Now.TimeOfDay.ToString()}");


            methodTrigger.Update();
            Thread.Sleep(50);
            methodTrigger.Update();
            Thread.Sleep(10);
            methodTrigger.Update();
            Thread.Sleep(10);
            beforeInsertedNewRecordTriggersTime.Add(DateTime.Now);
            methodTrigger.Inserted();
            Thread.Sleep(10);

            Thread.Sleep(1_000);
            _output.WriteLine("Slept 1 sec");

            // Should Get 1 Inserted

            _output.WriteLine($"triggers count: {triggers.Count}, suppose to be 1");

            Assert.Equal(1, triggers.Count, 0.5);

            List<double> deltas1 = beforeInsertedNewRecordTriggersTime.Zip(insertedNewRecordTriggersTime, (prev, next) => (next - prev).TotalSeconds).ToList();
            var avgDelta1 = deltas1.Average();
            var stdDev1 = Math.Sqrt(deltas1.Average(d => Math.Pow(d - avgDelta1, 2)));

            _output.WriteLine($"Avg Delta Time: {avgDelta1} seconds, Std: {stdDev1} seconds, Min: {deltas1.Min()} Max: {deltas1.Max()}");

            triggers.Clear();
            triggersTime.Clear();
            insertedNewRecordTriggersTime.Clear();
            beforeInsertedNewRecordTriggersTime.Clear();
        }

        private List<string> triggers = new List<string>();
        private List<DateTime> triggersTime = new List<DateTime>();
        private List<DateTime> insertedNewRecordTriggersTime = new List<DateTime>();
        private List<DateTime> beforeInsertedNewRecordTriggersTime = new List<DateTime>();

        private void MethodTrigger_FireTrigger(TriggerType triggerType)
        {
            try
            {
                var msg = $"Triggered: {triggerType}, {DateTime.Now.TimeOfDay.ToString()}";
                triggers.Add(msg);
                triggersTime.Add(DateTime.Now);
                if (triggerType == TriggerType.InsertedNewRecord)
                    insertedNewRecordTriggersTime.Add(DateTime.Now);
                _output.WriteLine(msg);
            }
            catch { }
        }

        [Fact]
        public void TablePublisherTest1()
        {
            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "TablePublisherTest1",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => new CommDummy(),
                20,
                200,
                1024 * 1204,
                new Logger((msg) => _output.WriteLine(msg)));

            tablePublisher.InsertRecord(new InternalPerson() { Id = 1, a = 12, b = 13, c = 14, d = 15 });
            tablePublisher.InsertRecord(new InternalPerson() { Id = 2, a = 22, b = 23, c = 24, d = 25 });
            tablePublisher.InsertRecord(new InternalPerson() { Id = 3, a = 32, b = 33, c = 34, d = 35 });
            tablePublisher.InsertRecord(new InternalPerson() { Id = 4, a = 42, b = 43, c = 44, d = 45 });

            Thread.Sleep(10_000);

            tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 0, b = 0, c = 0, d = 0 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 0, b = 0, c = 0, d = 0 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 0, b = 0, c = 0, d = 0 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 0, b = 0, c = 0, d = 0 });

            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 1, a = 0, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 2, a = 0, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 3, a = 0, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 4, a = 0, b = 0, c = 0, d = 0 });

            tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 112, b = 113, c = 14, d = 115 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 222, b = 223, c = 24, d = 225 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 332, b = 333, c = 34, d = 335 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 442, b = 443, c = 44, d = 445 });

            Thread.Sleep(10_000);

            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 1, a = 1112, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 2, a = 2222, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 3, a = 3332, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 4, a = 4442, b = 0, c = 0, d = 0 });

            tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 11112, b = 11113, c = 11114, d = 11115 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 22222, b = 22223, c = 22224, d = 22225 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 33332, b = 33333, c = 33334, d = 33335 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 44442, b = 44443, c = 44444, d = 44445 });

            Thread.Sleep(10_000);

            tablePublisher.pMimicClientRequestToGetFullPicture(100);

            Thread.Sleep(10_000);

            tablePublisher.RemoveRecord(new IdHolder() { Id = 1 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 2 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 3 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 4 });

            Thread.Sleep(10_000);

            var intPicture = tablePublisher.pGetInternalPicture();

            Assert.Equal(0, intPicture.Count);
        }

        public interface HasId
        {
            uint Id { get; set; }

        }

        public class IdHolder : HasId, IEquatable<IdHolder>
        {
            public uint Id { get; set; }

            public static void Encode(HasId idHolder, BinaryWriter bw)
            {
                bw.Write(idHolder.Id);
            }

            public static HasId Decode(BinaryReader br)
            {
                var a = new IdHolder();

                a.Id = br.ReadUInt32();

                return a;
            }

            public bool Equals(IdHolder? other) => other is not null && Id == other.Id;

            public override int GetHashCode() => HashCode.Combine(Id);

            public override bool Equals(object obj) => obj is IdHolder other && Equals(other);

          }

        public class InternalPerson : HasId
        {
            public DateTime LastUpdateTime = DateTime.Now;

            public uint Id { get; set; }

            public int a = 0;
            public int b = 0;
            public int c = 0;
            public int d = 0;
            public byte[] array;

            public InternalPerson() 
            {
                array = new byte[5000];
            }

            public InternalPerson(bool noArray)
            {
                array = null;
            }

            public InternalPerson Clone()
            {
                var clone = new InternalPerson();

                clone.Id = Id;

                clone.LastUpdateTime = LastUpdateTime;
                clone.a = a;
                clone.b = b;
                clone.c = c;
                clone.d = d;

                for (int i = 0; i<array.Length; i++)
                    clone.array[i] = array[i];

                return clone;
            }

            public InternalPerson SlimClone()
            {
                var clone = new InternalPerson(false);

                clone.Id = Id;
                clone.LastUpdateTime = LastUpdateTime;
                clone.a = a;

                return clone;
            }
        }

        public class ExternalPerson : HasId
        {
            public uint Id { get; set; }

            public DateTime LastUpdateTime = DateTime.Now;

            public int a = 0;
            public int b = 0;
            public int c = 0;
            public byte[] array = new byte[5000];

            public static void Encode(ExternalPerson externalPerson, BinaryWriter bw)
            {
                bw.Write(externalPerson.Id);
                bw.Write(externalPerson.a);
                bw.Write(externalPerson.b);
                bw.Write(externalPerson.c);
                bw.Write(externalPerson.array.Length);
                for (int i=0; i<externalPerson.array.Length; i++)
                    bw.Write(externalPerson.array[i]);

                bw.Write(externalPerson.LastUpdateTime.ToBinary());
            }

            public static ExternalPerson Decode(BinaryReader br)
            {
                var a = new ExternalPerson();

                a.Id = br.ReadUInt32();
                a.a = br.ReadInt32();
                a.b = br.ReadInt32();
                a.c = br.ReadInt32();

                var externalPersonArrayLength = br.ReadInt32();
                for (int i = 0; i < externalPersonArrayLength; i++)
                    a.array[i] = br.ReadByte();

                a.LastUpdateTime = DateTime.FromBinary(br.ReadInt64());

                return a;
            }
        }

        public class SlimExternalPerson : HasId
        {
            public uint Id { get; set; }

            public int a = 0;

            public static void Encode(SlimExternalPerson slimExternalPerson, BinaryWriter bw)
            {
                bw.Write(slimExternalPerson.Id);
                bw.Write(slimExternalPerson.a);
            }

            public static SlimExternalPerson Decode(BinaryReader br)
            {
                var a = new SlimExternalPerson();

                a.Id = br.ReadUInt32();
                a.a = br.ReadInt32();

                return a;
            }
        }

        public class CommDummy : IServerCommunication
        {
            public event Action<Int128> OnNewClient;
            public event Action<Int128, byte[], long> OnNewClientMessage;

            private Int128 guid = 100;

            public List<Int128> GetClients()
            {
                return new List<Int128>() { guid };
            }

            public void SendDataToClient(Int128 clientId, byte[] data, long count)
            {
                var a = new DataToSend<ExternalPerson, SlimExternalPerson, HasId>();
                a.Deserialize(data, ExternalPerson.Decode, SlimExternalPerson.Decode, IdHolder.Decode);
            }

            public void Init(long maxMessageSize)
            {
            }

            public void Dispose()
            {
            }
        }

        public class Logger : ILogger
        {
            private Action<string> _action;

            public Logger(Action<string> action)
            {
                _action = action;
            }

            public void Error(Exception ex)
            {
                _action(ex.ToString());
            }

            public void Error(string message)
            {
                _action(message);
            }

            public void Info(string message)
            {
                _action(message);
            }

            public void Warning(string message)
            {
                _action(message);
            }
        }
    }
}
 