using RP.Communication.ServerClient.Interface;
using RP.Infra.Logger;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace RP.TablePublisher
{
    public class RecordAction<TRecord, TId>
    {
        public ActionType Action { get; set; }
        public TRecord Record { get; set; }
        public TId Id { get; set; }
    }

    public enum ClientRequestType
    {
        NotRelevant,
        FullPicture,
    }

    public class ClientRequest
    {
        public Int128 ClientId { get; set; }
        public ClientRequestType Request { get; set; }
        public bool IsToAllClients { get; set; }
        public bool PublishToLocalEvent { get; set; }
    }

    public enum PrepareDataToSendTriggerType
    {
        //Timer = 1,
        ClientRequest = 2,
        InsertedRecord = 4,
        //TimerToSendIdsAndRevisions = 8,
        RegularUpdate = 16,
    }

    public class FullAndSlim<T>
    {
        public T Full { get; set; }
        public T Slim { get; set; }
    }    

    public class TablePublisher<TIn, TOut, TOutSlim, TId> : ITablePublisher<TIn, TOut, TOutSlim, TId>
    {
        private ILogger _logger;

        private Guid guid;

        private Func<TIn, TOut> _convertToOutType;
        private Func<TIn, TOutSlim> _convertToSlimOutType;
        
        private Action<TOut, BinaryWriter> _serializeToOutType;
        private Action<TOutSlim, BinaryWriter> _serializeToSlimOutType;
        private Action<TId, BinaryWriter> _serializeToIdType;

        private Func<TIn, TId> _extractId;

        private ConcurrentQueue<RecordAction<TIn, TId>> records = new();
        private ConcurrentQueue<ClientRequest> clientRequests = new();

        private Dictionary<TId, Tuple<FullAndSlim<TIn>, uint>> internalRecordsPicture = new();

        private IServerCommunication serverCommunication;

        private MethodTrigger methodTrigger;

        private byte[] preAllocatedBuffer = new byte[0];

        public bool Available => ToMonitorTypeConverter != null;

        public string TableName { get; private set; }

        public Guid TableGuid { get { return guid; } }

        public Func<List<Tuple<TOut, TOutSlim, uint>>, List<List<string>>> ToMonitorTypeConverter { get; set; }

        public List<Tuple<TOut, TOutSlim, uint>> recordsForCache;
        private ManualResetEvent recordsForCacheReady = new ManualResetEvent(false);

        public void Initiliaze(
            string tableName,
            Func<TIn, TOut> convertToOutType,
            Func<TIn, TOutSlim> convertToSlimOutType,
            Action<TOut,     BinaryWriter> serializeToOutType,
            Action<TOutSlim, BinaryWriter> serializeToSlimOutType,
            Action<TId, BinaryWriter> serializeToIdType,
            Func<TIn, TId> extractId,
            Func<IServerCommunication> serverCommunicationCreator,
            int periodicSyncToClientsTimeSec, 
            int minTimeBetweenUpdateSents_mSec,
            long maxMessageSize, 
            ILogger logger)
        {
            TableName = tableName;

            _logger = logger;

            guid = Guid.NewGuid();

            _convertToOutType = convertToOutType;
            _convertToSlimOutType = convertToSlimOutType;
            _serializeToOutType = serializeToOutType;
            _serializeToSlimOutType = serializeToSlimOutType;
            _serializeToIdType = serializeToIdType;

            _extractId = extractId;

            methodTrigger = new MethodTrigger(periodicSyncToClientsTimeSec, minTimeBetweenUpdateSents_mSec);
            methodTrigger.FireTrigger += MethodTrigger_FireTrigger;

            preAllocatedBuffer = new byte[maxMessageSize];

            serverCommunication = serverCommunicationCreator();
            serverCommunication.Init(maxMessageSize);

            serverCommunication.OnNewClient -= ServerCommunication_OnNewClient;
            serverCommunication.OnNewClient += ServerCommunication_OnNewClient;
            
            serverCommunication.OnNewClientMessage -= ServerCommunication_OnNewClientMessage;
            serverCommunication.OnNewClientMessage += ServerCommunication_OnNewClientMessage;

            ProberCacheClub.ProberCacheClubSingleton.Register(this);
        }

        private void ServerCommunication_OnNewClientMessage(Int128 clientId, byte[] buffer, long count)
        {
            var clientDataToSend = new ClientDataToSend();
            clientDataToSend.Deserialize(buffer);
            if (clientDataToSend.RequestType == ClientMessageType.RequestFullPicture)
            {
                clientRequests.Enqueue(new ClientRequest() { ClientId = clientId, Request = ClientRequestType.FullPicture, IsToAllClients = false });
                TriggerRun(PrepareDataToSendTriggerType.ClientRequest);
            }
        }

        private void MethodTrigger_FireTrigger(TriggerType triggerType)
        {
            var data = PerpareDataToSend(triggerType);

            var bytesCount = data.Item2.Serialize(
                ref preAllocatedBuffer,
                _serializeToOutType,
                _serializeToSlimOutType,
                _serializeToIdType);
            
            if (!(data.Item1.Count == 1 && data.Item1[0].IsToAllClients && data.Item1[0].Request == ClientRequestType.NotRelevant))
            {
                foreach (var client in data.Item1)
                {
                    try
                    {
                        if (!client.PublishToLocalEvent)
                        {
                            serverCommunication.SendDataToClient(client.ClientId, preAllocatedBuffer, bytesCount);
                        }
                        else
                        {
                            recordsForCache = data.Item2.FullPicture.Records;
                            recordsForCacheReady.Set();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.Error(ex);
                    }
                }                
            }
            else
            {
                try
                {
                    var clients = serverCommunication.GetClients();

                    foreach (var client in clients)
                    {
                        try
                        {
                            serverCommunication.SendDataToClient(client, preAllocatedBuffer, bytesCount);
                        }
                        catch (Exception ex)
                        {
                            _logger?.Error(ex);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger?.Error(ex);
                }
            }

        }

        private void TriggerRun(PrepareDataToSendTriggerType trigger)
        {
            switch (trigger)
            {
                case PrepareDataToSendTriggerType.RegularUpdate:
                    methodTrigger?.Update();
                    break;
                case PrepareDataToSendTriggerType.InsertedRecord:
                    methodTrigger?.Inserted();
                    break;
                case PrepareDataToSendTriggerType.ClientRequest:
                    methodTrigger?.ClientRequests();
                    break;
            }
        }
        
        private void ServerCommunication_OnNewClient(Int128 clientId)
        {
            clientRequests.Enqueue(new ClientRequest() { ClientId = clientId, Request = ClientRequestType.FullPicture, IsToAllClients = false });
            TriggerRun(PrepareDataToSendTriggerType.ClientRequest);
        }

        public void pMimicClientRequestToGetFullPicture(uint clientId)
        {
            clientRequests.Enqueue(new ClientRequest() { ClientId = clientId, Request = ClientRequestType.FullPicture, IsToAllClients = false });
            TriggerRun(PrepareDataToSendTriggerType.ClientRequest);
        }

        public Dictionary<TId, Tuple<FullAndSlim<TIn>, uint>> pGetInternalPicture()
        {
            return internalRecordsPicture;
        }

        public void InsertRecord(TIn record)
        {            
            records.Enqueue(new RecordAction<TIn, TId>() { Action = ActionType.Insert, Record = record, Id = _extractId(record) });
            TriggerRun(PrepareDataToSendTriggerType.InsertedRecord);
        }

        public void RemoveRecord(TId recordId)
        {
            records.Enqueue(new RecordAction<TIn, TId>() { Action = ActionType.Remove, Id = recordId });
            TriggerRun(PrepareDataToSendTriggerType.RegularUpdate);
        }

        public void UpdateRecord(TIn record)
        {
            records.Enqueue(new RecordAction<TIn, TId>() { Action = ActionType.Update, Record = record, Id = _extractId(record) });
            TriggerRun(PrepareDataToSendTriggerType.RegularUpdate);
        }

        public void SlimUpdateRecord(TIn record)
        {
            records.Enqueue(new RecordAction<TIn, TId>() { Action = ActionType.SlimUpdate, Record = record, Id = _extractId(record) });
            TriggerRun(PrepareDataToSendTriggerType.RegularUpdate);
        }

        private Tuple<List<ClientRequest>, DataToSend<TOut, TOutSlim, TId>> PerpareDataToSend(TriggerType trigger)
        {
            Tuple<List<ClientRequest>, DataToSend<TOut, TOutSlim, TId>> dataToSend = null;

            {

                if (trigger == TriggerType.PeriodicIdsAndRevisions)
                {
                    dataToSend = new Tuple<List<ClientRequest>, DataToSend<TOut, TOutSlim, TId>>(new List<ClientRequest>() { new ClientRequest() { IsToAllClients = true } }, new DataToSend<TOut, TOutSlim, TId>());
                    dataToSend.Item2.Type = UpdateMessageType.IdsAndRevisions;
                    dataToSend.Item2.ServerGuid = guid;
                    dataToSend.Item2.IdsAndRevisions = new IdsAndRevisionsMessage<TId>() { RecrodsIdsAndRevisions = internalRecordsPicture.Select(x => Tuple.Create(x.Key, x.Value.Item2)).ToList() };
                }
                else if (trigger == TriggerType.FullPictureToSpecificClients)
                {
                    var clientRequestsCount = clientRequests.Count;
                    var clientReq = new List<ClientRequest>(clientRequestsCount);

                    for (int i = 0; i < clientRequestsCount; i++)
                        if (clientRequests.TryDequeue(out ClientRequest clReq))
                            clientReq.Add(clReq);

                    dataToSend = new Tuple<List<ClientRequest>, DataToSend<TOut, TOutSlim, TId>>(new List<ClientRequest>(), new DataToSend<TOut, TOutSlim, TId>());
                    dataToSend.Item1.AddRange(clientReq);

                    dataToSend.Item2.Type = UpdateMessageType.FullPicture;
                    dataToSend.Item2.ServerGuid = guid;

                    var recordsToSend = internalRecordsPicture.Values.Select(x => Tuple.Create(_convertToOutType(x.Item1.Full), x.Item1.Slim != null ? _convertToSlimOutType(x.Item1.Slim) : default, x.Item2)).ToList();
                    dataToSend.Item2.FullPicture = new FullPictureMessage<TOut, TOutSlim>();
                    dataToSend.Item2.FullPicture.Records.AddRange(recordsToSend);
                }
                else // TriggerType.ForceTrigger || TriggerType.Regular || TriggerType.InsertedNewRecord
                {
                    dataToSend = new Tuple<List<ClientRequest>, DataToSend<TOut, TOutSlim, TId>>(new List<ClientRequest>() { new ClientRequest() { IsToAllClients = true } }, new DataToSend<TOut, TOutSlim, TId>());
                    dataToSend.Item2.Type = UpdateMessageType.Update;
                    dataToSend.Item2.ServerGuid = guid;

                    dataToSend.Item2.Update = new UpdateMessage<TOut, TOutSlim, TId>();

                    var recordsCount = records.Count;
                    var recordsActions = new Dictionary<TId, List<RecordAction<TIn, TId>>>();

                    for (int i = 0; i < recordsCount; i++)
                    {
                        if (records.TryDequeue(out RecordAction<TIn, TId> rec))
                        {
                            if (!recordsActions.ContainsKey(rec.Id))
                                recordsActions.Add(rec.Id, new List<RecordAction<TIn, TId>>());

                            recordsActions[rec.Id].Add(rec);
                        }
                    }

                    var recordsActionsFinal = new List<RecordAction<TIn, TId>>();

                    foreach (var recordActions in recordsActions)
                    {
                        var hasInsert = recordActions.Value[0].Action == ActionType.Insert;
                        var hasRemove = recordActions.Value[recordActions.Value.Count - 1].Action == ActionType.Remove;

                        if (hasInsert && hasRemove)
                            continue;

                        if (hasRemove)
                        {
                            recordsActionsFinal.Add(recordActions.Value[recordActions.Value.Count - 1]);
                            continue;
                        }

                        RecordAction<TIn, TId> update = null;
                        RecordAction<TIn, TId> slimUpdate = null;
                        for (int i = recordActions.Value.Count - 1; i >= 0; i--)
                        {
                            if (recordActions.Value[i].Action == ActionType.Update)
                            {
                                update = recordActions.Value[i];
                                recordsActionsFinal.Add(update);
                                if (slimUpdate != null)
                                    recordsActionsFinal.Add(slimUpdate);
                                break;
                            }
                            else if (slimUpdate == null && recordActions.Value[i].Action == ActionType.SlimUpdate)
                            {
                                slimUpdate = recordActions.Value[i];
                            }
                        }

                        if (update == null && hasInsert)
                        {
                            recordsActionsFinal.Add(recordActions.Value[0]);
                            if (slimUpdate != null)
                                recordsActionsFinal.Add(slimUpdate);
                        }
                        else if (update == null && slimUpdate != null)
                        {
                            recordsActionsFinal.Add(slimUpdate);
                        }

                    }

                    foreach (var recordAction in recordsActionsFinal) // recordsActions.Values)
                    {
                        switch (recordAction.Action)
                        {
                            case ActionType.Remove:
                                {
                                    if (internalRecordsPicture.ContainsKey(recordAction.Id))
                                    {
                                        internalRecordsPicture.Remove(recordAction.Id);
                                        dataToSend.Item2.Update.Removed.Add(recordAction.Id);
                                    }
                                }
                                break;
                            case ActionType.Insert:
                            case ActionType.Update:
                                {
                                    if (!internalRecordsPicture.ContainsKey(recordAction.Id))
                                    {
                                        internalRecordsPicture.Add(recordAction.Id, Tuple.Create<FullAndSlim<TIn>, uint>(new FullAndSlim<TIn>() { Full = recordAction.Record, Slim = default }, 0));
                                        dataToSend.Item2.Update.Inserted.Add(Tuple.Create<uint, TOut>(0, _convertToOutType(recordAction.Record)));
                                    }
                                    else
                                    {
                                        internalRecordsPicture[recordAction.Id] = Tuple.Create<FullAndSlim<TIn>, uint>(new FullAndSlim<TIn>() { Full = recordAction.Record, Slim = default }, internalRecordsPicture[recordAction.Id].Item2 + 1);
                                        dataToSend.Item2.Update.Updated.Add(Tuple.Create<uint, TOut>(internalRecordsPicture[recordAction.Id].Item2, _convertToOutType(recordAction.Record)));
                                    }
                                }
                                break;

                            case ActionType.SlimUpdate:
                                {
                                    if (!internalRecordsPicture.ContainsKey(recordAction.Id))
                                    {
                                        //internalRecordsPicture.Add(recordAction.Id, Tuple.Create<TIn, uint>(recordAction.Record, 0));
                                        //dataToSend.Item2.Update.Inserted.Add(Tuple.Create<uint, TOut>(0, _convertToOutType(recordAction.Record)));
                                    }
                                    else if (internalRecordsPicture[recordAction.Id].Item1.Full != null)
                                    {
                                        internalRecordsPicture[recordAction.Id] =
                                            Tuple.Create<FullAndSlim<TIn>, uint>(
                                                new FullAndSlim<TIn>() { Full = internalRecordsPicture[recordAction.Id].Item1.Full, Slim = recordAction.Record },
                                                internalRecordsPicture[recordAction.Id].Item2 + 1);

                                        dataToSend.Item2.Update.SlimUpdated.Add(Tuple.Create<uint, TOutSlim>(internalRecordsPicture[recordAction.Id].Item2, _convertToSlimOutType(recordAction.Record)));
                                    }
                                }
                                break;
                        }
                    }
                }
            }

            return dataToSend;
        }
        
        public void Dispose()
        {
            ProberCacheClub.ProberCacheClubSingleton.Unregister(this);

            serverCommunication.Dispose();
            methodTrigger.StopPeriodicTask();
            methodTrigger = null;
        }

        public List<List<string>> GetInnerCachedTable()
        {
            var list = new List<List<string>>();
            
            if (Available)
            {
                
                recordsForCacheReady.Reset();
                clientRequests.Enqueue(new ClientRequest() { ClientId = 0, Request = ClientRequestType.FullPicture, IsToAllClients = false, PublishToLocalEvent = true });
                TriggerRun(PrepareDataToSendTriggerType.ClientRequest);
                recordsForCacheReady.WaitOne();

                var local_RecordsForCache = recordsForCache;
                recordsForCache = null;

                var res = ToMonitorTypeConverter?.Invoke(local_RecordsForCache);
                if (res != null)
                    list = res;
            }

            return list;

        }
    }

    public enum UpdateMessageType
    {
        Update,
        IdsAndRevisions,
        FullPicture,
        //Sync,
    }

    public class DataToSend<TOut, TOutSlim, TId>
    {
        public Guid ServerGuid { get; set; }
        public UpdateMessageType Type { get; set; }
        public UpdateMessage<TOut, TOutSlim, TId> Update { get; set; }
        public IdsAndRevisionsMessage<TId> IdsAndRevisions { get; set; }
        public FullPictureMessage<TOut, TOutSlim> FullPicture { get; set; }
        //public SyncMessage Sync { get; set; }

        public long Serialize(
            ref byte[] messageInBytes,
            Action<TOut, BinaryWriter> serializeToOutType,
            Action<TOutSlim, BinaryWriter> serializeToSlimOutType,
            Action<TId, BinaryWriter> serializeToId)
        {
            long bytesCount = 0;

            using (var memoryStream = messageInBytes.Length == 0 ? new MemoryStream() : new MemoryStream(messageInBytes))
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                Encode(binaryWriter, serializeToOutType, serializeToSlimOutType, serializeToId);
                
                messageInBytes = memoryStream.ToArray();
                bytesCount = memoryStream.Position;
            }            

            return bytesCount;
        }

        public void Encode(
            BinaryWriter binaryWriter,
            Action<TOut, BinaryWriter> serializeToOutType,
            Action<TOutSlim, BinaryWriter> serializeToSlimOutType,
            Action<TId, BinaryWriter> serializeToId)
        {
            unsafe
            {
                {
                    var guid = ServerGuid;
                    byte* byteArray = (byte*)&guid;

                    binaryWriter.Write(new Span<byte>(byteArray, 16));
                    binaryWriter.Write((byte)Type);
                    switch(Type)
                    {
                        case UpdateMessageType.Update:
                            Update.Encode(binaryWriter, serializeToOutType, serializeToSlimOutType, serializeToId);
                            break;
                        case UpdateMessageType.FullPicture:
                            FullPicture.Encode(binaryWriter, serializeToOutType, serializeToSlimOutType);
                            break;
                        case UpdateMessageType.IdsAndRevisions:
                            IdsAndRevisions.Encode(binaryWriter, serializeToId);
                            break;
                    }                    
                }
            }
        }

        public void Deserialize(
            byte[] messageInBytes,
            Func<BinaryReader, TOut> deserializeToOutType,
            Func<BinaryReader, TOutSlim> deserializeToSlimOutType,
            Func<BinaryReader, TId> deserializeToId)
        {
            if (messageInBytes.Length == 0)
                return;

            using (var memoryStream = new MemoryStream(messageInBytes))
            using (var binaryReader = new BinaryReader(memoryStream))
            {
                Decode(binaryReader, deserializeToOutType, deserializeToSlimOutType, deserializeToId);
            }
        }

        public void Decode(
            BinaryReader binaryReader,
            Func<BinaryReader, TOut> deserializeToOutType,
            Func<BinaryReader, TOutSlim> deserializeToSlimOutType,
            Func<BinaryReader, TId> deserializeToId)
        {
            unsafe
            {
                {
                    Span<byte> buffer = stackalloc byte[16];
                    binaryReader.Read(buffer);

                    ServerGuid = new Guid(buffer);

                    Type = (UpdateMessageType) binaryReader.ReadByte();
                    
                    switch (Type)
                    {
                        case UpdateMessageType.Update:
                            Update = new UpdateMessage<TOut, TOutSlim, TId>();
                            Update.Decode(binaryReader, deserializeToOutType, deserializeToSlimOutType, deserializeToId);
                            break;
                        case UpdateMessageType.FullPicture:
                            FullPicture = new FullPictureMessage<TOut, TOutSlim>();
                            FullPicture.Decode(binaryReader, deserializeToOutType, deserializeToSlimOutType);
                            break;
                        case UpdateMessageType.IdsAndRevisions:
                            IdsAndRevisions = new IdsAndRevisionsMessage<TId>();
                            IdsAndRevisions.Decode(binaryReader, deserializeToId);
                            break;
                    }
                }
            }
        }
    }

    public class UpdateMessage<TOut, TOutSlim, TId>
    {       
        public List<Tuple<uint,TOut>> Inserted = new List<Tuple<uint, TOut>>();
        public List<Tuple<uint, TOut>> Updated = new List<Tuple<uint, TOut>>();
        public List<Tuple<uint, TOutSlim>> SlimUpdated = new List<Tuple<uint, TOutSlim>>();
        public List<TId> Removed = new List<TId>();

        public void Encode(
            BinaryWriter binaryWriter,
            Action<TOut, BinaryWriter> serializeToOutType,
            Action<TOutSlim, BinaryWriter> serializeToSlimOutType,
            Action<TId, BinaryWriter> serializeToId)
        {
            binaryWriter.Write(Inserted.Count);

            for (int i=0; i<Inserted.Count; i++)
            {
                binaryWriter.Write(Inserted[i].Item1);
                serializeToOutType(Inserted[i].Item2, binaryWriter);
            }

            binaryWriter.Write(Updated.Count);

            for (int i = 0; i < Updated.Count; i++)
            {
                binaryWriter.Write(Updated[i].Item1);
                serializeToOutType(Updated[i].Item2, binaryWriter);
            }

            binaryWriter.Write(SlimUpdated.Count);

            for (int i = 0; i < SlimUpdated.Count; i++)
            {
                binaryWriter.Write(SlimUpdated[i].Item1);
                serializeToSlimOutType(SlimUpdated[i].Item2, binaryWriter);
            }

            binaryWriter.Write(Removed.Count);

            for (int i = 0; i < Removed.Count; i++)
            {
                serializeToId(Removed[i], binaryWriter);
            }
        }

        public void Decode(
            BinaryReader binaryReader,
            Func<BinaryReader, TOut> deserializeToOutType,
            Func<BinaryReader, TOutSlim> deserializeToSlimOutType,
            Func<BinaryReader, TId> deserializeToId)
        {
            var insertedCount = binaryReader.ReadInt32();            

            for (int i = 0; i < insertedCount; i++)
            {
                uint item1 = binaryReader.ReadUInt32(); // revision id
                TOut item2 = deserializeToOutType(binaryReader);

                Inserted.Add(Tuple.Create(item1, item2));
            }

            var updatedCount = binaryReader.ReadInt32();

            for (int i = 0; i < updatedCount; i++)
            {
                uint item1 = binaryReader.ReadUInt32(); // revision id
                TOut item2 = deserializeToOutType(binaryReader);

                Updated.Add(Tuple.Create(item1, item2));
            }

            var slimUpdatedCount = binaryReader.ReadInt32();

            for (int i = 0; i < slimUpdatedCount; i++)
            {
                uint item1 = binaryReader.ReadUInt32(); // revision id
                TOutSlim item2 = deserializeToSlimOutType(binaryReader);

                SlimUpdated.Add(Tuple.Create(item1, item2));
            }

            var removedCount = binaryReader.ReadInt32();

            for (int i = 0; i < removedCount; i++)
            {
                Removed.Add(deserializeToId(binaryReader));
            }
        }
    }

    public class IdsAndRevisionsMessage<TId>
    {
        public List<Tuple<TId, uint>> RecrodsIdsAndRevisions = new List<Tuple<TId, uint>>();

        public void Encode(
            BinaryWriter binaryWriter,
            Action<TId, BinaryWriter> serializeToId)
        {
            binaryWriter.Write(RecrodsIdsAndRevisions.Count);

            for (int i = 0; i < RecrodsIdsAndRevisions.Count; i++)
            {                
                serializeToId(RecrodsIdsAndRevisions[i].Item1, binaryWriter);
                binaryWriter.Write(RecrodsIdsAndRevisions[i].Item2);
            }
        }

        public void Decode(
            BinaryReader binaryReader,
            Func<BinaryReader, TId> deserializeToId)
        {
            var recrodsIdsAndRevisionsCount = binaryReader.ReadInt32();

            for (int i = 0; i < recrodsIdsAndRevisionsCount; i++)
            {
                var item1 = deserializeToId(binaryReader);
                var item2 = binaryReader.ReadUInt32();

                RecrodsIdsAndRevisions.Add(Tuple.Create(item1, item2));
            }
        }
    }

    public class FullPictureMessage<TOut, TOutSlim>
    {
        public List<Tuple<TOut, TOutSlim, uint>> Records = new List<Tuple<TOut, TOutSlim, uint>>();

        public void Encode(
            BinaryWriter binaryWriter,
            Action<TOut, BinaryWriter> serializeToOutType,
            Action<TOutSlim, BinaryWriter> serializeToSlimOutType)
        {
            binaryWriter.Write(Records.Count);

            for (int i = 0; i < Records.Count; i++)
            {
                serializeToOutType(Records[i].Item1, binaryWriter);
                var isThereSlimOut = Records[i].Item2 != null;
                binaryWriter.Write(isThereSlimOut);
                if (isThereSlimOut)
                    serializeToSlimOutType(Records[i].Item2, binaryWriter);
                binaryWriter.Write(Records[i].Item3);
            }
        }

        public void Decode(
            BinaryReader binaryReader,
            Func<BinaryReader, TOut> deserializeToOutType,
            Func<BinaryReader, TOutSlim> deserializeToSlimOutType)
        {
            var recordsCount = binaryReader.ReadInt32();

            for (int i = 0; i < recordsCount; i++)
            {
                var tOut = deserializeToOutType(binaryReader);
                TOutSlim tSlimOut = default;
                var isThereTSlimOut = binaryReader.ReadBoolean();
                if (isThereTSlimOut)
                    tSlimOut = deserializeToSlimOutType(binaryReader);
                uint revision = binaryReader.ReadUInt32();

                Records.Add(Tuple.Create<TOut, TOutSlim, uint>(tOut, tSlimOut, revision));
            }
        }
    }

    //public class SyncMessage { }
}
