using Microsoft.VisualStudio.TestPlatform.Utilities;
using RP.Communication.ServerClient.Interface;
using RP.Infra.Logger;
using StateOfTheArtTablePublisher;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;
using static PublisherXUnitTests.StateOfTheArtTablePublisherTest;

namespace PublisherXUnitTests
{
    public class StateOfTheArtTablePubSubTest
    {
        private readonly ITestOutputHelper _output;

        public StateOfTheArtTablePubSubTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void PubSubTest1()
        {
            var inMemoryCommunication = new InMemoryCommunication();
            var client1Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            client1Comm.ConnectToServer();

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
                "PubSubTest1",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => inMemoryCommunication,
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

            tablePublisher.pMimicClientRequestToGetFullPicture(100);

            clientInnerTable = tableSubscriber.GetInnerTable();

            Thread.Sleep(10_000);

            tablePublisher.RemoveRecord(new IdHolder() { Id = 1 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 2 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 3 });
            tablePublisher.RemoveRecord(new IdHolder() { Id = 4 });

            Thread.Sleep(10_000);

            var intPicture = tablePublisher.pGetInternalPicture();

            clientInnerTable = tableSubscriber.GetInnerTable();

            Assert.Equal(clientInnerTable.Count, intPicture.Count);
        }

        [Fact]
        public void PubSub_N_Records()
        {
            int nRecords = 500;

            var inMemoryCommunication = new InMemoryCommunication();
            var client1Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            client1Comm.ConnectToServer();

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
                () => inMemoryCommunication,
                20,
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

            // Thread.Sleep(10_000);
            //
            // var clientInnerTable = tableSubscriber.GetInnerTable();
            //
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 0, b = 0, c = 0, d = 0 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 0, b = 0, c = 0, d = 0 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 0, b = 0, c = 0, d = 0 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 0, b = 0, c = 0, d = 0 });
            //
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 1, a = 0, b = 0, c = 0, d = 0 });
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 2, a = 0, b = 0, c = 0, d = 0 });
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 3, a = 0, b = 0, c = 0, d = 0 });
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 4, a = 0, b = 0, c = 0, d = 0 });
            //
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 112, b = 113, c = 14, d = 115 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 222, b = 223, c = 24, d = 225 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 332, b = 333, c = 34, d = 335 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 442, b = 443, c = 44, d = 445 });
            //
            // Thread.Sleep(10_000);
            //
            // clientInnerTable = tableSubscriber.GetInnerTable();
            //
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 1, a = 1112, b = 0, c = 0, d = 0 });
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 2, a = 2222, b = 0, c = 0, d = 0 });
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 3, a = 3332, b = 0, c = 0, d = 0 });
            // tablePublisher.SlimUpdateRecord(new InternalPerson() { Id = 4, a = 4442, b = 0, c = 0, d = 0 });
            //
            // Thread.Sleep(10_000);
            //
            // clientInnerTable = tableSubscriber.GetInnerTable();
            //
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 1, a = 11112, b = 11113, c = 11114, d = 11115 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 2, a = 22222, b = 22223, c = 22224, d = 22225 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 3, a = 33332, b = 33333, c = 33334, d = 33335 });
            // tablePublisher.UpdateRecord(new InternalPerson() { Id = 4, a = 44442, b = 44443, c = 44444, d = 44445 });
            //
            // Thread.Sleep(10_000);
            //
            // tablePublisher.pMimicClientRequestToGetFullPicture(new ClientDetail() { ClientId = Guid.NewGuid() });
            //
            // clientInnerTable = tableSubscriber.GetInnerTable();
            //
            // Thread.Sleep(10_000);
            //
            // tablePublisher.RemoveRecord(new IdHolder() { Id = 1 });
            // tablePublisher.RemoveRecord(new IdHolder() { Id = 2 });
            // tablePublisher.RemoveRecord(new IdHolder() { Id = 3 });
            // tablePublisher.RemoveRecord(new IdHolder() { Id = 4 });
            //
            // Thread.Sleep(10_000);
            //
            // var intPicture = tablePublisher.pGetInternalPicture();
            //
            // clientInnerTable = tableSubscriber.GetInnerTable();
            //
            // Assert.Equal(clientInnerTable.Count, intPicture.Count);
        }

        [Fact]
        public void PubSub_N_Records_UpdateAsInsertForLowLatency()
        {
            int nRecords = 500;

            var inMemoryCommunication = new InMemoryCommunication();
            var client1Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            //client1Comm.ConnectToServer();

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
                "PubSub_N_Records_UpdateAsInsertForLowLatency",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => inMemoryCommunication,
                20,
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
                            tablePublisher.InsertRecord(person.Clone());

                            for (int k = 0; k < 10; k++)
                            {
                                person.a++;
                                person.LastUpdateTime = DateTime.Now;
                                tablePublisher.InsertRecord(person.SlimClone());
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
                        var maxDelay = clientInnerTable.Select(x => DateTime.Now.Subtract(x.LastUpdateTime).TotalSeconds).Max();
                        //Console.WriteLine($"Max Delay Sec: {maxDelay} ");
                        _output.WriteLine($"Max Delay Sec: {maxDelay} ");
                    }
                }, TaskCreationOptions.LongRunning);

            Thread.Sleep(50_000);

            var sentPerTotalUpdates = persons[0].a;
            var clientInnerTableLast = tableSubscriber.GetInnerTable();
            var receivedPerTotalUpdates = clientInnerTableLast[0].a;

            _output.WriteLine($"Total Updated {sentPerTotalUpdates}, LastReceived {receivedPerTotalUpdates}");
        }

        [Fact]
        public void PubSub_N_Records_MissingMessages()
        {
            int nRecords = 1050;

            var inMemoryCommunication = new InMemoryCommunication(true, 30);
            var client1Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            client1Comm.ConnectToServer();

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
                () => inMemoryCommunication,
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
            var inMemoryCommunication = new InMemoryCommunication(true, int.MaxValue);
            var client1Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            client1Comm.ConnectToServer();

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
                    () => inMemoryCommunication,
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

            var inMemoryCommunication = new InMemoryCommunication(true, int.MaxValue);
            var client1Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            client1Comm.ConnectToServer();
            var client2Comm = new InMemoryCommunicationClient(inMemoryCommunication);
            client2Comm.ConnectToServer();

            var tablePublisher = new TablePublisher<InternalPerson, ExternalPerson, SlimExternalPerson, HasId>();
            tablePublisher.Initiliaze(
                "PubSub_Client_Late_Connect",
                a => new ExternalPerson() { Id = a.Id, a = a.a, b = a.b, c = a.c },
                a => new SlimExternalPerson() { Id = a.Id, a = a.a },
                ExternalPerson.Encode,
                SlimExternalPerson.Encode,
                IdHolder.Encode,
                a => new IdHolder() { Id = a.Id },
                () => inMemoryCommunication,
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
                new Logger((msg) => _output.WriteLine(msg)));

            Thread.Sleep(2000);

            var clientInnerTable = tableSubscriber.GetInnerTable();

            _output.WriteLine($"SupposeToBe: {nRecords}, TotalAtClientSide {clientInnerTable.Count}");


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
                new Logger((msg) => _output.WriteLine(msg)));
            
            Thread.Sleep(2000);
            
            var clientInnerTable1 = tableSubscriber1.GetInnerTable();
            
            _output.WriteLine($"SupposeToBe: {nRecords}, TotalAtClientSide {clientInnerTable1.Count}");
        }
    }

    public class InMemoryCommunicationClient : IClientCommunication
    {
        public InMemoryCommunication server;
        private uint clientId;
        private static uint clientIdGenerator = 0;

        public InMemoryCommunicationClient(InMemoryCommunication server)
        {
            this.server = server;
            clientId = Interlocked.Increment(ref clientIdGenerator);
        }

        public event Action<byte[], long> NewMessageArrived;

        public void InvokeNewMessageArrived(byte[] data, long count)
        {
            NewMessageArrived?.Invoke(data, count);
        }

        public void ConnectToServer()
        {
            server.ConnectToServer(clientId, this);
        }

        public void Init(long maxMessageSize)
        {
        }

        public void SendDataToServer(byte[] data, long count)
        {
            server.SendDataToServer(clientId, data, count);
        }
    }

    public class InMemoryCommunication : IServerCommunication //, IClientCommunication
    {
        public event Action<Int128> OnNewClient;
        
        public event Action<Int128, byte[], long> OnNewClientMessage;
        //public event Action<byte[], long> NewMessageArrived;

        private Dictionary<Int128, InMemoryCommunicationClient> clientGuids = new();

        private bool addMissingMessagesSomeTimes;
        private int messagesCounter;
        private int messagesCountToMiss;

        private ILogger logger = null;


        
        public InMemoryCommunication(
            bool addMissingMessagesSomeTimes = false, 
            int messagesCountToMiss = int.MaxValue,
            ILogger logger = null)
        {
            this.addMissingMessagesSomeTimes = addMissingMessagesSomeTimes;
            this.messagesCountToMiss = messagesCountToMiss;
            this.logger = logger;
        }

        public List<Int128> GetClients()
        {
            return clientGuids.Keys.ToList();
        }

        public void SendDataToClient(Int128 clientId, byte[] data, long count)
        {
            var client = clientGuids[clientId];

            //var dataToSend = new DataToSend<ExternalPerson, SlimExternalPerson, HasId>();
            //dataToSend.Deserialize(data, ExternalPerson.Decode, SlimExternalPerson.Decode, IdHolder.Decode);
            messagesCounter++;

            if (messagesCounter % messagesCountToMiss == 0)
            {
                //logger?.Info("InMemoryCommunication: Force Missing Message");
                return;
            }

            client.InvokeNewMessageArrived(data, count);
        }

        public void Init(long maxMessageSize)
        {
        }

        public void ConnectToServer(Int128 clientId, InMemoryCommunicationClient client)
        {            
            clientGuids.Add(clientId, client);
            OnNewClient?.Invoke(clientId);
        }

        public void SendDataToServer(Int128 clientId, byte[] data, long count)
        {
            OnNewClientMessage?.Invoke(clientId, data, count); 
        }

        public void Dispose()
        {
        }
    }
}
