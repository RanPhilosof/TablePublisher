using Microsoft.VisualStudio.TestPlatform.Utilities;
using RP.Communication.ServerClient.Interface;
using StateOfTheArtTablePublisher;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tcp.Communication.ByteArray.ServerClient;
using Udp.Communication.ByteArray.ServerClient;
using Xunit.Abstractions;
using static PublisherXUnitTests.StateOfTheArtTablePublisherTest;

namespace TablePubSubWithRealCommTests
{
    public class TablePubSubWithRealCommTests
    {
        private bool useTcpComm = true;

        private readonly ITestOutputHelper _output;

        public TablePubSubWithRealCommTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void PubSubTest11()
        {
            IClientCommunication client1Comm;
            IServerCommunication serverComm;
            Func<uint> serverGetSentMessagesCount;
            Func<uint> clientGetReceivedMessagesCount;

            if (!useTcpComm)
            {
                serverComm = new UdpByteArrayServer(7000, 8400, 20_000, 10 * 1024 * 1024, 5); // new Logger((msg) => _output.WriteLine(msg)));
                client1Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                serverGetSentMessagesCount = (serverComm as UdpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as UdpByteArrayClient).GetTotalReceivedMessages;
            }
            else
            {
                serverComm = new TcpByteArrayServer(7000, 20_000, 10 * 1024 * 1024, 5);
                client1Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);

                serverGetSentMessagesCount = (serverComm as TcpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as TcpByteArrayClient).GetTotalReceivedMessages;
            }


            var tableSubscriber = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();

            tableSubscriber.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;

            tableSubscriber.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client1Comm,
                1024 * 1024,
                new Logger((msg) => _output.WriteLine(msg)));

            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "PubSubTest11",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => serverComm,
                20,
                200,
                1024 * 1204,
                new Logger((msg) => _output.WriteLine(msg)));

            tablePublisher.InsertRecord(new InternalPerson() { Id = 1, a = 12, b = 13, c = 14, d = 15 });
            tablePublisher.InsertRecord(new InternalPerson() { Id = 2, a = 22, b = 23, c = 24, d = 25 });
            tablePublisher.InsertRecord(new InternalPerson() { Id = 3, a = 32, b = 33, c = 34, d = 35 });
            tablePublisher.InsertRecord(new InternalPerson() { Id = 4, a = 42, b = 43, c = 44, d = 45 });

            Thread.Sleep(10_000);

            var clientInnerTable = tableSubscriber.GetInnerTable();

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

            clientInnerTable = tableSubscriber.GetInnerTable();

            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 1, a = 1112, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 2, a = 2222, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 3, a = 3332, b = 0, c = 0, d = 0 });
            tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 4, a = 4442, b = 0, c = 0, d = 0 });

            Thread.Sleep(10_000);

            clientInnerTable = tableSubscriber.GetInnerTable();

            tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 11112, b = 11113, c = 11114, d = 11115 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 22222, b = 22223, c = 22224, d = 22225 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 33332, b = 33333, c = 33334, d = 33335 });
            tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 44442, b = 44443, c = 44444, d = 44445 });

            Thread.Sleep(10_000);

            tablePublisher.pMimicClientRequestToGetFullPicture(2);

            var intPicture = tablePublisher.pGetInternalPicture();

            clientInnerTable = tableSubscriber.GetInnerTable();

            Assert.Equal(clientInnerTable.Count, intPicture.Count);

            Thread.Sleep(10_000);

            tablePublisher.RemoveRecord(new IdHolder() { Id = 1 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 2 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 3 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 4 });

            Thread.Sleep(10_000);

            intPicture = tablePublisher.pGetInternalPicture();

            clientInnerTable = tableSubscriber.GetInnerTable();

            Assert.Equal(clientInnerTable.Count, intPicture.Count);
        }

        [Fact]
        public void PubSub_N_Records()
        {
            int nRecords = 500;

            IClientCommunication client1Comm;
            IServerCommunication serverComm;
            Func<uint> serverGetSentMessagesCount;
            Func<uint> clientGetReceivedMessagesCount;

            if (!useTcpComm)
            {
                serverComm = new UdpByteArrayServer(7000, 8400, 20_000, 10 * 1024 * 1024, 5); // new Logger((msg) => _output.WriteLine(msg)));
                client1Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                serverGetSentMessagesCount = (serverComm as UdpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as UdpByteArrayClient).GetTotalReceivedMessages;
            }
            else
            {
                serverComm = new TcpByteArrayServer(7000, 20_000, 10 * 1024 * 1024, 5);
                client1Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);

                serverGetSentMessagesCount = (serverComm as TcpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as TcpByteArrayClient).GetTotalReceivedMessages;
            }

            var tableSubscriber = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();

            tableSubscriber.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;

            tableSubscriber.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client1Comm,
                10 * 1024 * 1024,
                new Logger((msg) => _output.WriteLine(msg)));

            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "PubSub_N_Records",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => serverComm,
                20,
                200,
                10 * 1024 * 1204,
                new Logger((msg) => _output.WriteLine(msg)));

            tablePublisher.ToMonitorTypeConverter =
                (persons) =>
                    {
                        var personsList = new List<List<string>>();                        
                        personsList?.Add(new List<string>() { "Revision", "LastUpdateTime", "Id", "A", "B", "C" });
                        
                        foreach (var person in persons)
                        {
                            var personInfo = new List<string>();
                            if (personInfo != null)
                            {
                                personInfo.Add(person.Item3.ToString());
                                personInfo.Add(person.Item1.LastUpdateTime.ToShortTimeString());
                                personInfo.Add(person.Item1.Id.ToString());

                                if (person.Item2 != null)
                                    personInfo.Add(person.Item2.a.ToString());
                                else
                                    personInfo.Add(person.Item1.a.ToString());

                                personInfo.Add(person.Item1.b.ToString());
                                personInfo.Add(person.Item1.c.ToString());

                                personsList?.Add(personInfo);
                            }

                        }

                        return personsList!;
                    };

            var persons = new List<InternalPerson>();

            for (int i = 0; i < nRecords; i++)
            {
                var newPerson = new InternalPerson()
                {
                    Id = (uint)i,
                    a = i,
                    b = i,
                    c = i,
                    d = i
                };

                for (int j = 0; j < newPerson.array.Length; j++)
                    newPerson.array[j] = (byte)i;

                persons.Add(newPerson);
            }

            foreach (var person in persons)
                tablePublisher.InsertRecord(person.Clone());

            Task.Factory.StartNew(
                () =>
                {
                    while (true)
                    {
                        foreach (var person in persons)
                        {
                            person.a++;
                            person.LastUpdateTime = DateTime.Now;
                            tablePublisher.UpdateRecord(person.Clone());

                            for (int k = 0; k < 10; k++)
                            {
                                person.a++;
                                person.LastUpdateTime = DateTime.Now;
                                tablePublisher.UpdateRecord(person.SlimClone());
                            }
                        }
                    }
                }, TaskCreationOptions.LongRunning);

            Task.Factory.StartNew(
                () =>
                {
                    while (true)
                    {
                        Thread.Sleep(1000);
                        var clientInnerTable = tableSubscriber.GetInnerTable();
                        if (clientInnerTable.Count > 0)
                        {
                            var maxDelay = clientInnerTable.Select(x => DateTime.Now.Subtract(x.LastUpdateTime).TotalSeconds).Max();
                            _output.WriteLine($"Max Delay Sec: {maxDelay}");
                        }
                        else
                        {
                            _output.WriteLine($"Max Delay Sec: No Elements");
                        }
                    }
                }, TaskCreationOptions.LongRunning);

            Thread.Sleep(50_000);

            var sentPerTotalUpdates = persons[0].a;
            var clientInnerTableLast = tableSubscriber.GetInnerTable();
            var receivedPerTotalUpdates = clientInnerTableLast[0].a;

            _output.WriteLine($"Total Updated {sentPerTotalUpdates}, LastReceived {receivedPerTotalUpdates}");

            var cachedTablesNames = ProberCacheClub.ProberCacheClubSingleton.GetCachedTablesNames().Select(x => x.Guid).ToList();
            var cachedTables = ProberCacheClub.ProberCacheClubSingleton.GetCachedTables(cachedTablesNames);

        }

        [Fact]
        public void PubSub_N_Records_UpdateAsInsertForLowLatency()
        {
            int nRecords = 500;

            IClientCommunication client1Comm;
            IServerCommunication serverComm;
            Func<uint> serverGetSentMessagesCount;
            Func<uint> clientGetReceivedMessagesCount;

            if (!useTcpComm)
            {
                serverComm = new UdpByteArrayServer(7000, 8400, 20_000, 10 * 1024 * 1024, 5); // new Logger((msg) => _output.WriteLine(msg)));
                client1Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                serverGetSentMessagesCount = (serverComm as UdpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as UdpByteArrayClient).GetTotalReceivedMessages;
            }
            else
            {
                serverComm = new TcpByteArrayServer(7000, 20_000, 10 * 1024 * 1024, 5);
                client1Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);
                
                serverGetSentMessagesCount = (serverComm as TcpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as TcpByteArrayClient).GetTotalReceivedMessages;
            }

            var tableSubscriber = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();

            tableSubscriber.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;

            tableSubscriber.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client1Comm,
                100 * 1024 * 1024,
                new Logger((msg) => _output.WriteLine($"[Client] - {msg}")));

            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "PubSub_N_Records_UpdateAsInsertForLowLatency",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => serverComm,
                20,
                200,
                100 * 1024 * 1204,
                new Logger((msg) => _output.WriteLine($"[Server] - {msg}")));

            var persons = new List<InternalPerson>();

            for (int i = 0; i < nRecords; i++)
            {
                var newPerson = new InternalPerson()
                {
                    Id = (uint)i,
                    a = i,
                    b = i,
                    c = i,
                    d = i
                };

                for (int j = 0; j < newPerson.array.Length; j++)
                    newPerson.array[j] = (byte)i;

                persons.Add(newPerson);
            }

            foreach (var person in persons)
                tablePublisher.InsertRecord(person.Clone());

            Task.Factory.StartNew(
                () =>
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    while (true)
                    {
                        var time1Sec = sw.Elapsed.TotalSeconds;

                        foreach (var person in persons)
                        {
                            person.a++;
                            person.LastUpdateTime = DateTime.Now;
                            tablePublisher.InsertRecord(person.Clone());

                            for (int k = 0; k < 10; k++)
                            {
                                person.a++;
                                person.LastUpdateTime = DateTime.Now;
                                tablePublisher.InsertRecord(person.SlimClone());
                            }
                        }

                        var time2Sec = sw.Elapsed.TotalSeconds;

                        var deltaTime = Math.Max(0, time2Sec - time1Sec);

                        //Thread.Sleep((int) (deltaTime * 1000));
                    }
                }, TaskCreationOptions.LongRunning);

            Task.Factory.StartNew(
                () =>
                {
                    while (true)
                    {
                        Thread.Sleep(1000);
                        var clientInnerTable = tableSubscriber.GetInnerTable();
                        if (clientInnerTable.Count > 0)
                        {
                            var maxDelay = clientInnerTable.Select(x => DateTime.Now.Subtract(x.LastUpdateTime).TotalSeconds).Max();
                            //Console.WriteLine($"Max Delay Sec: {maxDelay} ");
                            _output.WriteLine($"Max Delay Sec: {maxDelay} ");
                        }
                    }
                }, TaskCreationOptions.LongRunning);

            Thread.Sleep(50_000);

            var sentPerTotalUpdates = persons[0].a;
            var clientInnerTableLast = tableSubscriber.GetInnerTable();
            var receivedPerTotalUpdates = clientInnerTableLast[0].a;

            _output.WriteLine($"Total Updated {sentPerTotalUpdates}, LastReceived {receivedPerTotalUpdates}");
            _output.WriteLine($"Total Messages Sent {serverGetSentMessagesCount()}, Total Messages Received {clientGetReceivedMessagesCount()}");
        }

        [Fact]
        public void PubSub_N_Records_MissingMessages()
        {
            int nRecords = 1050;

            IClientCommunication client1Comm;
            IServerCommunication serverComm;
            Func<uint> serverGetSentMessagesCount;
            Func<uint> clientGetReceivedMessagesCount;

            if (!useTcpComm)
            {
                serverComm = new UdpByteArrayServer(7000, 8400, 20_000, 10 * 1024 * 1024, 5); // new Logger((msg) => _output.WriteLine(msg)));
                client1Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                serverGetSentMessagesCount = (serverComm as UdpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as UdpByteArrayClient).GetTotalReceivedMessages;
            }
            else
            {
                serverComm = new TcpByteArrayServer(7000, 20_000, 10 * 1024 * 1024, 5);
                client1Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);
                
                (serverComm as TcpByteArrayServer).SetMissingMessagesToClient(true, 30);

                serverGetSentMessagesCount = (serverComm as TcpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as TcpByteArrayClient).GetTotalReceivedMessages;
            }

            var tableSubscriber = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();

            tableSubscriber.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;

            tableSubscriber.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client1Comm,
                10 * 1024 * 1024,
                new Logger((msg) => _output.WriteLine(msg)));

            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "PubSub_N_Records_MissingMessages",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => serverComm,
                5,
                200,
                10 * 1024 * 1204,
                new Logger((msg) => _output.WriteLine(msg)));

            var persons = new List<InternalPerson>();

            for (int i = 0; i < nRecords; i++)
            {
                var newPerson = new InternalPerson()
                {
                    Id = (uint)i,
                    a = i,
                    b = i,
                    c = i,
                    d = i
                };

                for (int j = 0; j < newPerson.array.Length; j++)
                    newPerson.array[j] = (byte)i;

                persons.Add(newPerson);
            }

            foreach (var person in persons)
            {
                tablePublisher.InsertRecord(person.Clone());
                Thread.Sleep(10);
            }

            Thread.Sleep(25000);
            var clientInnerTable = tableSubscriber.GetInnerTable();

            _output.WriteLine($"SupposeToBe: {nRecords}, TotalAtClientSide {clientInnerTable.Count}");
        }

        private void TableSubscriber_OnNewUpdateBatch(UpdateBatch<ExternalPerson, SlimExternalPerson, HasId> batch)
        {
            Console.WriteLine(batch.ToString());
        }

        [Fact]
        public void PubSub_ServerCrash_Test()
        {
            IClientCommunication client1Comm;
            Func<uint> clientGetReceivedMessagesCount;

            if (!useTcpComm)
            {
                client1Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                clientGetReceivedMessagesCount = (client1Comm as UdpByteArrayClient).GetTotalReceivedMessages;
            }
            else
            {
                client1Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);
                clientGetReceivedMessagesCount = (client1Comm as TcpByteArrayClient).GetTotalReceivedMessages;
            }

            var tableSubscriber = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();

            tableSubscriber.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;

            tableSubscriber.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client1Comm,
                10 * 1024 * 1024,
                new Logger((msg) => _output.WriteLine(msg)));

            for (int k = 0; k < 10; k++)
            {
                IServerCommunication serverComm;
                Func<uint> serverGetSentMessagesCount;

                if (!useTcpComm)
                {
                    serverComm = new UdpByteArrayServer(7000, 8400, 20_000, 10 * 1024 * 1024, 5); // new Logger((msg) => _output.WriteLine(msg)));
                    serverGetSentMessagesCount = (serverComm as UdpByteArrayServer).GetTotalSentMessages;
                }
                else
                {
                    serverComm = new TcpByteArrayServer(7000, 20_000, 10 * 1024 * 1024, 5);
                    serverGetSentMessagesCount = (serverComm as TcpByteArrayServer).GetTotalSentMessages;
                }

                int nRecords = 5 + k * 50;

                var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
                tablePublisher.Initiliaze(
                    "PubSub_ServerCrash_Test",
                    a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                    a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                    ExternalPerson.Encode,
                    SlimExternalPerson.Encode,
                    IdHolder.Encode,
                    a => new IdHolder() { Id = a.Id },
                    () => serverComm,
                    5,
                    200,
                    10 * 1024 * 1204,
                    new Logger((msg) => _output.WriteLine(msg)));

                var persons = new List<InternalPerson>();

                for (int i = 0; i < nRecords; i++)
                {
                    var newPerson = new InternalPerson()
                    {
                        Id = (uint)i,
                        a = i,
                        b = i,
                        c = i,
                        d = i
                    };

                    for (int j = 0; j < newPerson.array.Length; j++)
                        newPerson.array[j] = (byte)i;

                    persons.Add(newPerson);
                }

                foreach (var person in persons)
                {
                    tablePublisher.InsertRecord(person.Clone());
                    Thread.Sleep(10);
                }

                Thread.Sleep(11000);
                var clientInnerTable = tableSubscriber.GetInnerTable();

                _output.WriteLine($"SupposeToBe: {nRecords}, TotalAtClientSide {clientInnerTable.Count}");

                tablePublisher.Dispose();
            }
        }

        [Fact]
        public void PubSub_Client_Late_Connect()
        {
            int nRecords = 1000;

            IClientCommunication client1Comm;
            IClientCommunication client2Comm;
            IServerCommunication serverComm;
            Func<uint> serverGetSentMessagesCount;
            Func<uint> clientGetReceivedMessagesCount;

            if (!useTcpComm)
            {
                serverComm = new UdpByteArrayServer(7000, 8400, 20_000, 10 * 1024 * 1024, 5); // new Logger((msg) => _output.WriteLine(msg)));
                client1Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                client2Comm = new UdpByteArrayClient("192.168.1.177", 7000, 8400, 10 * 1024 * 1024); // new Logger((msg) => _output.WriteLine(msg)));
                serverGetSentMessagesCount = (serverComm as UdpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as UdpByteArrayClient).GetTotalReceivedMessages;
            }
            else
            {
                serverComm = new TcpByteArrayServer(7000, 20_000, 10 * 1024 * 1024, 5);
                client1Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);
                client2Comm = new TcpByteArrayClient("127.0.0.1", 7000, 10 * 1024 * 1024);

                serverGetSentMessagesCount = (serverComm as TcpByteArrayServer).GetTotalSentMessages;
                clientGetReceivedMessagesCount = (client1Comm as TcpByteArrayClient).GetTotalReceivedMessages;
            }

            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "PubSub_Client_Late_Connect",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => serverComm,
                5,
                200,
                10 * 1024 * 1204,
                new Logger((msg) => _output.WriteLine($"[{DateTime.Now.TimeOfDay.ToString()}][TablePublisher] - {msg}")));

            var persons = new List<InternalPerson>();

            for (int i = 0; i < nRecords; i++)
            {
                var newPerson = new InternalPerson()
                {
                    Id = (uint)i,
                    a = i,
                    b = i,
                    c = i,
                    d = i
                };

                for (int j = 0; j < newPerson.array.Length; j++)
                    newPerson.array[j] = (byte)i;

                persons.Add(newPerson);
            }

            foreach (var person in persons)
            {
                tablePublisher.InsertRecord(person.Clone());
                Thread.Sleep(10);
            }

            Thread.Sleep(2000);

            var tableSubscriber = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();

            tableSubscriber.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;

            tableSubscriber.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client1Comm,
                10 * 1024 * 1024,
                new Logger((msg) => _output.WriteLine($"[{DateTime.Now.TimeOfDay.ToString()}][TableSubscriber_0] - {msg}")));

            Thread.Sleep(2000);

            var clientInnerTable = tableSubscriber.GetInnerTable();

            _output.WriteLine($"[{DateTime.Now.TimeOfDay.ToString()}] SupposeToBe: {nRecords}, TotalAtClientSide {clientInnerTable.Count}");


            var tableSubscriber1 = new TableSubscriber<ExternalPerson, SlimExternalPerson, HasId>();
            
            tableSubscriber1.OnNewUpdateBatch += TableSubscriber_OnNewUpdateBatch;
            
            tableSubscriber1.Initiliaze(
                (extPer, sliExt) => { extPer.a = sliExt.a; return extPer; },
                ExternalPerson.Decode,
                SlimExternalPerson.Decode,
                IdHolder.Decode,
                a => new IdHolder() { Id = a.Id },
                a => new IdHolder() { Id = a.Id },
                () => client2Comm,
                10 * 1024 * 1024,
                new Logger((msg) => _output.WriteLine($"[{DateTime.Now.TimeOfDay.ToString()}][TableSubscriber_1] - {msg}")));

            Thread.Sleep(2000);
            
            var clientInnerTable1 = tableSubscriber1.GetInnerTable();
            
            _output.WriteLine($"[{DateTime.Now.TimeOfDay.ToString()}] SupposeToBe: {nRecords}, TotalAtClientSide {clientInnerTable1.Count}");
        }
    }
}
