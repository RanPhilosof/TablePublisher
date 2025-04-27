using RP.Communication.ServerClient.Interface;
using RP.Infra.Logger;
using RP.TablePublisher;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace StateOfTheArtTablePublisher
{
    public class TableSubscriber<TOut, TOutSlim, TId> : ITableSubscriber<TOut, TOutSlim, TId>
    {
        private ILogger _logger;

        private IClientCommunication _clientComm;
        private Func<BinaryReader, TOut> _deserializeToOutType;
        private Func<BinaryReader, TOutSlim> _deserializeToSlimOutType;
        private Func<BinaryReader, TId> _deserializeToIdType;

        private Func<TOut, TOutSlim, TOut> _mergeSlimUpdateToOutType;

        public event Action<UpdateBatch<TOut, TOutSlim, TId>> OnNewUpdateBatch;

        private Dictionary<TId, Tuple<TOut, uint>> tableValues = new Dictionary<TId, Tuple<TOut, uint>>();
        private ConcurrentQueue<DataToSend<TOut, TOutSlim, TId>> receviedDataQueue = new ConcurrentQueue<DataToSend<TOut, TOutSlim, TId>>();

        private Func<TOut, TId> _extractId;
        private Func<TOutSlim, TId> _extractIdFromSlim;

        private Guid lastServerGuid = Guid.Empty;

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private ManualResetEvent manualResetEvent = new ManualResetEvent(false);

        private Guid guid = Guid.NewGuid();

        private byte[] buffer = new byte[4096];

        public List<TOut> GetInnerTable()
        {
            var t = new List<TOut>();

            lock (tableValues)
            {
                t = tableValues.Select(x => x.Value.Item1).ToList();
            }

            return t;
        }

        public void Initiliaze(
            Func<TOut, TOutSlim, TOut> mergeSlimUpdateToOutType, 
            Func<BinaryReader, TOut> deserializeToOutType, 
            Func<BinaryReader, TOutSlim> deserializeToSlimOutType, 
            Func<BinaryReader, TId> deserializeToIdType, 
            Func<TOut, TId> extractId, 
            Func<TOutSlim, TId> extractIdFromSlim, 
            Func<IClientCommunication> clientCommunicationCreator, 
            long maxMessageSize,
            ILogger logger)
        {
            _logger = logger;

            _clientComm = clientCommunicationCreator();

            _clientComm.Init(maxMessageSize);

            _clientComm.NewMessageArrived += _clientComm_NewMessageArrived;

            _clientComm.ConnectToServer();

            _deserializeToOutType = deserializeToOutType;
            _deserializeToSlimOutType = deserializeToSlimOutType;
            _deserializeToIdType = deserializeToIdType;

            _mergeSlimUpdateToOutType = mergeSlimUpdateToOutType;

            _extractId = extractId;
            _extractIdFromSlim = extractIdFromSlim;

            CancellationToken token = cancellationTokenSource.Token;

            Task.Factory.StartNew(
                () =>
                {
                    while (!token.IsCancellationRequested)
                    {
                        manualResetEvent.WaitOne(2_000);
                        HandleData(out bool requestOfFullPictureNeeded);
                        
                        if (requestOfFullPictureNeeded)
                        {
                            var clientMsg = new ClientDataToSend() { ClientGuid = guid, RequestType = ClientMessageType.RequestFullPicture };
                            var length = clientMsg.Serialize(ref buffer);
                            _clientComm.SendDataToServer(buffer, length);

                            _logger?.Info($"Client {guid}: Requesting full picture from server");
                        }

                        manualResetEvent.Reset();
                    }
                },
                TaskCreationOptions.LongRunning);
        }        

        private void _clientComm_NewMessageArrived(byte[] data, long length)
        {
            var receivedData = new DataToSend<TOut, TOutSlim, TId>();
            receivedData.Deserialize(data, _deserializeToOutType, _deserializeToSlimOutType, _deserializeToIdType);

            receviedDataQueue.Enqueue(receivedData);
            manualResetEvent.Set();
        }
        
        private void HandleData(out bool requestOfFullPictureNeeded)
        {
            requestOfFullPictureNeeded = false;

            var dataToHandle = new List<DataToSend<TOut, TOutSlim, TId>>();
            var count = receviedDataQueue.Count;
            for (int i=0; i<count; i++)
                if (receviedDataQueue.TryDequeue(out DataToSend<TOut, TOutSlim, TId> data))
                    dataToHandle.Add(data);
          
            bool removeAllOldPictureBecauseOfNewServer = false;

            if (dataToHandle.Count == 0)
                return;

            if (lastServerGuid == Guid.Empty || dataToHandle.Last().ServerGuid != lastServerGuid)
            {
                var index = dataToHandle.FindLastIndex(x => x.Type == UpdateMessageType.FullPicture);
                if (index == -1)
                {
                    requestOfFullPictureNeeded = true;
                    removeAllOldPictureBecauseOfNewServer = true;
                }
                else
                {                    
                    dataToHandle = dataToHandle.Skip(index).ToList();

                    if (dataToHandle.Last().ServerGuid != dataToHandle.First().ServerGuid)
                    {
                        requestOfFullPictureNeeded = true;
                        removeAllOldPictureBecauseOfNewServer = true;
                    }
                    else
                    {
                        requestOfFullPictureNeeded = false;
                        removeAllOldPictureBecauseOfNewServer = true;
                    }
                }
                
                lastServerGuid = dataToHandle.Last().ServerGuid;
            }

            if (removeAllOldPictureBecauseOfNewServer)
            {
                var removeAllBatch = new UpdateBatch<TOut, TOutSlim, TId>();

                foreach (var key in tableValues.Keys)
                    removeAllBatch.Removed.Add(key);

                tableValues.Clear();

                OnNewUpdateBatch.Invoke(removeAllBatch);

                _logger?.Warning($"Client {guid}: Server Replaced. Removing Old Picture And Request Full Picture");

                if (requestOfFullPictureNeeded)
                    return;
            }

            #region if there is full picture in the messages remove all the messages before it.
            int indexToDropAllUntilIt = 0;
            for (int i = dataToHandle.Count - 1; i>=0 ; i--)
            {
                if (dataToHandle[i].Type == UpdateMessageType.FullPicture)
                {
                    _logger?.Info($"Client {guid}: Full Picture Message Arrived. Total Records: {dataToHandle[i].FullPicture.Records.Count}");
                    indexToDropAllUntilIt = i;
                    break;
                }
            }
            
            if (indexToDropAllUntilIt > 0)
                dataToHandle = dataToHandle.Skip(indexToDropAllUntilIt).ToList();
            #endregion if there is full picture in the messages remove all the messages before it.

            #region if there is more than one IdsAndRevisions keep only the last one.
            
            bool startRemovingIdsAndRevisions = false;
            for (int i = dataToHandle.Count - 1; i >= 0; i--)
            {
                if (startRemovingIdsAndRevisions && dataToHandle[i].Type == UpdateMessageType.IdsAndRevisions)
                    dataToHandle[i] = null;

                if (!startRemovingIdsAndRevisions && dataToHandle[i].Type == UpdateMessageType.IdsAndRevisions)
                    startRemovingIdsAndRevisions = true;
            }
            dataToHandle.RemoveAll(x => x == null);

            #endregion if there is more than one IdsAndRevisions keep only the last one.

            #region Compact Many Update Messages To One Update Message Between IdsAndRevisionsMessage
            dataToHandle = CombineConsecutiveUpdates(dataToHandle);
            #endregion Compact Many Update Messages To One Update Message Between IdsAndRevisionsMessage

            // Update Internal Picture While Building Insert, Update, Remove Events + Check If Request To FullPicture From Server Needed.
            var udpateBatch = new UpdateBatch<TOut, TOutSlim, TId>();

            for (int i = 0; i < dataToHandle.Count; i++)
            {
                switch (dataToHandle[i].Type)
                {
                    case UpdateMessageType.IdsAndRevisions:
                        if (dataToHandle[i].IdsAndRevisions.RecrodsIdsAndRevisions.Count() != tableValues.Count)
                        {
                            requestOfFullPictureNeeded = true;                            
                        }
                        else
                        {
                            foreach (var idAndRev in dataToHandle[i].IdsAndRevisions.RecrodsIdsAndRevisions)
                            {
                                if (!tableValues.ContainsKey(idAndRev.Item1))
                                {
                                    requestOfFullPictureNeeded = true;
                                    break;
                                }
                                else if (tableValues[idAndRev.Item1].Item2 != idAndRev.Item2)
                                {
                                    requestOfFullPictureNeeded = true;
                                    break;
                                }
                            }
                        }
                        if (requestOfFullPictureNeeded)
                            _logger?.Warning($"Client {guid}: Ids And Revisions Mismatch.");
                        break;
                    case UpdateMessageType.Update:
                        lock (tableValues)
                        {
                            foreach (var inserted in dataToHandle[i].Update.Inserted)
                            {
                                var id = _extractId(inserted.Item2);
                                if (!tableValues.ContainsKey(id))
                                { 
                                    tableValues.Add(id, Tuple.Create(inserted.Item2, inserted.Item1));
                                    if (!udpateBatch.Inserted.ContainsKey(id))
                                        udpateBatch.Inserted.Add(id, inserted.Item2);
                                    else
                                        udpateBatch.Inserted[id] = inserted.Item2;
                                }
                                else
                                {
                                    tableValues[id] = Tuple.Create(inserted.Item2, inserted.Item1);
                                    if (!udpateBatch.Updated.ContainsKey(id))
                                        udpateBatch.Updated.Add(id, inserted.Item2);
                                    else
                                        udpateBatch.Updated[id] = inserted.Item2;
                                }
                            }
                            foreach (var updated in dataToHandle[i].Update.Updated)
                            {
                                var id = _extractId(updated.Item2);
                                if (!tableValues.ContainsKey(id))
                                {
                                    tableValues.Add(id, Tuple.Create(updated.Item2, updated.Item1));
                                    if (!udpateBatch.Inserted.ContainsKey(id))
                                        udpateBatch.Inserted.Add(id, updated.Item2);
                                    else
                                        udpateBatch.Inserted[id] = updated.Item2;
                                }
                                else
                                {
                                    tableValues[id] = Tuple.Create(updated.Item2, updated.Item1);
                                    if (!udpateBatch.Updated.ContainsKey(id))
                                        udpateBatch.Updated.Add(id, updated.Item2);
                                    else
                                        udpateBatch.Updated[id] = updated.Item2;
                                }
                            }
                            foreach (var slimUpdated in dataToHandle[i].Update.SlimUpdated)
                            {
                                var id = _extractIdFromSlim(slimUpdated.Item2);
                                if (tableValues.ContainsKey(id))
                                {
                                    tableValues[id] = Tuple.Create(_mergeSlimUpdateToOutType(tableValues[id].Item1, slimUpdated.Item2), slimUpdated.Item1);
                                    
                                    if (!udpateBatch.SlimUpdated.ContainsKey(id))
                                        udpateBatch.SlimUpdated.Add(id, slimUpdated.Item2);
                                    else
                                        udpateBatch.SlimUpdated[id] = slimUpdated.Item2;
                                }
                            }
                            foreach (var removed in dataToHandle[i].Update.Removed)
                            {
                                if (tableValues.ContainsKey(removed))
                                {
                                    tableValues.Remove(removed);
                                    if (!udpateBatch.Removed.Contains(removed))
                                        udpateBatch.Removed.Add(removed);
                                }
                            }
                        }
                        break;
                    case UpdateMessageType.FullPicture:
                        lock (tableValues)
                        {
                            var currentPictureKeys = tableValues.Keys;
                            var fullPictureKeys = dataToHandle[i].FullPicture.Records.Select(x => _extractId(x.Item1)).ToHashSet();
                            foreach (var currentPictureKey in currentPictureKeys)
                            {
                                if (!fullPictureKeys.Contains(currentPictureKey))
                                {
                                    tableValues.Remove(currentPictureKey);
                                    if (!udpateBatch.Removed.Contains(currentPictureKey))
                                        udpateBatch.Removed.Add(currentPictureKey);
                                }
                            }

                            foreach (var record in dataToHandle[i].FullPicture.Records)
                            {
                                 var id = _extractId(record.Item1);

                                if (record.Item2 != null)
                                {
                                    if (!udpateBatch.SlimUpdated.ContainsKey(id))
                                        udpateBatch.SlimUpdated.Add(id, record.Item2);
                                }

                                if (!tableValues.ContainsKey(id))
                                {
                                    tableValues.Add(id, Tuple.Create(record.Item1, record.Item3));

                                    if (!udpateBatch.Inserted.ContainsKey(id))
                                        udpateBatch.Inserted.Add(id, record.Item1);
                                    else
                                        udpateBatch.Inserted[id] = record.Item1;

                                    if (udpateBatch.Updated.ContainsKey(id))
                                        udpateBatch.Updated.Remove(id);
                                }
                                else
                                {
                                    tableValues[id] = Tuple.Create(record.Item1, record.Item3);

                                    if (!udpateBatch.Updated.ContainsKey(id))
                                        udpateBatch.Updated.Add(id, record.Item1);
                                    else
                                        udpateBatch.Updated[id] = record.Item1;

                                    if (udpateBatch.Inserted.ContainsKey(id))
                                        udpateBatch.Inserted.Remove(id);
                                }

                                if (record.Item2 != null)
                                    tableValues[id] = Tuple.Create(_mergeSlimUpdateToOutType(record.Item1, record.Item2), record.Item3);
                            }
                            _logger?.Info($"Client {guid}: Full Picture Synced.");
                        }
                        break;
                }
            }
        }

        #region Compact Many Update Messages To One Update Message Between IdsAndRevisionsMessage
        private List<DataToSend<TOut, TOutSlim, TId>> CombineConsecutiveUpdates(List<DataToSend<TOut, TOutSlim, TId>> dataList)
        {
            var result = new List<DataToSend<TOut, TOutSlim, TId>>();
            var updateBuffer = new List<DataToSend<TOut, TOutSlim, TId>>();

            foreach (var item in dataList)
            {
                if (item.Type == UpdateMessageType.Update)
                {
                    updateBuffer.Add(item);
                }
                else
                {
                    if (updateBuffer.Any())
                    {
                        result.Add(MergeUpdates(updateBuffer));
                        updateBuffer.Clear();
                    }
                    result.Add(item); // Add "IdsAndRevisions" or "FullPicture" as-is
                }
            }

            if (updateBuffer.Any())
            {
                result.Add(MergeUpdates(updateBuffer));
            }

            return result;
        }

        private DataToSend<TOut, TOutSlim, TId> MergeUpdates(List<DataToSend<TOut, TOutSlim, TId>> updates)
        {
            var updatePerItem = new Dictionary<TId, List<UpdatesOnItem<TOut, TOutSlim, TId>>>();
            
            foreach (var update in updates)
            {
                foreach (var item in update.Update.Inserted)
                {
                    var id = _extractId(item.Item2);
                    if (!updatePerItem.ContainsKey(id))
                        updatePerItem.Add(id, new List<UpdatesOnItem<TOut, TOutSlim, TId>>());

                    updatePerItem[id].Add(new UpdatesOnItem<TOut, TOutSlim, TId> { Id = id, Out = item.Item2, Revision = item.Item1, Type = ActionType.Insert });
                }

                foreach (var item in update.Update.Updated)
                {
                    var id = _extractId(item.Item2);
                    if (!updatePerItem.ContainsKey(id))
                        updatePerItem.Add(id, new List<UpdatesOnItem<TOut, TOutSlim, TId>>());

                    updatePerItem[id].Add(new UpdatesOnItem<TOut, TOutSlim, TId> { Id = id, Out = item.Item2, Revision = item.Item1, Type = ActionType.Update });
                }

                foreach (var item in update.Update.SlimUpdated)
                {
                    var id = _extractIdFromSlim(item.Item2);
                    if (!updatePerItem.ContainsKey(id))
                        updatePerItem.Add(id, new List<UpdatesOnItem<TOut, TOutSlim, TId>>());

                    updatePerItem[id].Add(new UpdatesOnItem<TOut, TOutSlim, TId> { Id = id, OutSlim = item.Item2, Revision = item.Item1, Type = ActionType.SlimUpdate });
                }

                foreach (var item in update.Update.Removed)
                {
                    var id = item;
                    if (!updatePerItem.ContainsKey(id))
                        updatePerItem.Add(id, new List<UpdatesOnItem<TOut, TOutSlim, TId>>());

                    updatePerItem[id].Add(new UpdatesOnItem<TOut, TOutSlim, TId> { Id = id, OutSlim = default, Revision = default, Type = ActionType.Remove });
                }
            }

            foreach (var item in updatePerItem)
            {
                // Check if there is a remove operation
                var hasRemove = item.Value.Any(u => u.Out == null && u.OutSlim == null);
                if (hasRemove)
                {
                    updatePerItem[item.Key] = item.Value.Where(u => u.Out == null && u.OutSlim == null).ToList();
                    continue;
                }

                // Find the last Out in the list
                int lastOutIndex = item.Value.FindLastIndex(u => u.Out != null);
                if (lastOutIndex != -1)
                {
                    // Keep the last Out and all following OutSlim entries
                    updatePerItem[item.Key] = item.Value.Skip(lastOutIndex).ToList();
                }
                else 
                {
                    int lastOutSlimIndex = item.Value.FindLastIndex(u => u.OutSlim != null);
                    if (lastOutSlimIndex != -1)
                        updatePerItem[item.Key] = item.Value.Skip(lastOutSlimIndex).ToList();
                }
            }

            var result = new DataToSend<TOut, TOutSlim, TId>() 
            { 
                Type = UpdateMessageType.Update,
                Update = new UpdateMessage<TOut, TOutSlim, TId>(),
                ServerGuid = updates.First().ServerGuid, // Only one server Guid can enter this method.
            };
            

            foreach (var item in updatePerItem)
            {
                foreach (var element in item.Value)
                {
                    switch(element.Type)
                    {
                        case ActionType.Update:
                            result.Update.Updated.Add(Tuple.Create(element.Revision, element.Out));
                            break;
                        case ActionType.SlimUpdate:
                            result.Update.SlimUpdated.Add(Tuple.Create(element.Revision, element.OutSlim));
                            break;
                        case ActionType.Insert:
                            result.Update.Inserted.Add(Tuple.Create(element.Revision, element.Out));
                            break;
                        case ActionType.Remove:
                            result.Update.Removed.Add(element.Id);
                            break;
                    }
                        
                }
            }

            return result;
        }
        #endregion Compact Many Update Messages To One Update Message Between IdsAndRevisionsMessage
    }

    public class UpdatesOnItem<TOut, TOutSlim, TId>
    {
        public ActionType Type;
        public uint Revision;
        public TOut Out;
        public TOutSlim OutSlim;
        public TId Id;
    }
}
