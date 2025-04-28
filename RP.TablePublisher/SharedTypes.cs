using RP.Communication.ServerClient.Interface;
using RP.Infra.Logger;
using RP.Prober.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace RP.TablePublisherSubscriber
{
    public enum ActionType
    {
        Insert,
        Remove,
        Update,
        SlimUpdate,
    }

    public class UpdateBatch<TOut, TOutSlim, TId>
    {
        public Dictionary<TId, TOut> Inserted = new Dictionary<TId, TOut>();
        public Dictionary<TId, TOut> Updated = new Dictionary<TId, TOut>();
        public Dictionary<TId, TOutSlim> SlimUpdated = new Dictionary<TId, TOutSlim>();
        public HashSet<TId> Removed = new HashSet<TId>();
    }

    public interface ITableSubscriber<TOut, TOutSlim, TId>
    {
        void Initiliaze(
            Func<TOut, TOutSlim, TOut> convertToOutType,
            Func<BinaryReader, TOut> deserializeToOutType,
            Func<BinaryReader, TOutSlim> deserializeToSlimOutType,
            Func<BinaryReader, TId> deserializeToIdType,
            Func<TOut, TId> extractId,
            Func<TOutSlim, TId> extractIdFromSlim,
            Func<IClientCommunication> serverCommunicationCreator,
            long maxMessageSize,
            ILogger logger);

        event Action<UpdateBatch<TOut, TOutSlim, TId>> OnNewUpdateBatch;
        List<TOut> GetInnerTable();
    }



    public interface IProberCacheMonitoringTypeConverter<TOut, TOutSlim>
    {
        Func<List<Tuple<TOut, TOutSlim, uint>>, List<List<string>>> ToMonitorTypeConverter { get; set; }
    }

    public interface ITablePublisher<TIn, TOut, TOutSlim, TId> : IProberCacheMonitoring, IProberCacheMonitoringTypeConverter<TOut, TOutSlim>, IDisposable
    {                
        void Initiliaze(
            string tableName,
            Func<TIn, TOut> convertToOutType,
            Func<TIn, TOutSlim> convertToSlimOutType,
            Action<TOut, BinaryWriter> serializeToOutType,
            Action<TOutSlim, BinaryWriter> serializeToSlimOutType,
            Action<TId, BinaryWriter> serializeToIdType,
            Func<TIn, TId> extractId,
            Func<IServerCommunication> serverCommunicationCreator,
            int periodicSyncToClientsTimeSec,
            int minTimeBetweenUpdateSents_mSec,
            long maxMessageSize,
            ILogger logger);

        void InsertRecord(TIn record);
        void RemoveRecord(TId recordId);
        void UpdateRecord(TIn record);
        void SlimUpdateRecord(TIn record);
    }

    public class ClientDetail
    {
        public uint ClientId { get; set; }
    }



    public class NonBlockingCollection<T> where T : Enum
    {
        private ConcurrentDictionary<T, object> _items = new ConcurrentDictionary<T, object>();

        private object garbage = new object();

        public void Enqueue(T item)
        {
            _items[item] = garbage;
        }

        public List<T> Dequeue()
        {
            var list = new List<T>();

            list.AddRange(_items.Keys);

            foreach (var t in list)
                _items.TryRemove(t, out var removedItem);

            return list;
        }
    }
    public enum ClientMessageType
    {
        RequestFullPicture,
    }

    public class ClientDataToSend
    {
        public Guid ClientGuid { get; set; }
        public ClientMessageType RequestType { get; set; }

        public long Serialize(ref byte[] messageInBytes)
        {
            long bytesCount = 0;

            using (var memoryStream = messageInBytes.Length == 0 ? new MemoryStream() : new MemoryStream(messageInBytes))
            using (var binaryWriter = new BinaryWriter(memoryStream))
            {
                Encode(binaryWriter);

                messageInBytes = memoryStream.ToArray();
                bytesCount = memoryStream.Position;
            }

            return bytesCount;
        }

        public void Encode(BinaryWriter binaryWriter)
        {
            unsafe
            {
                {
                    var guid = ClientGuid;
                    byte* byteArray = (byte*)&guid;

                    binaryWriter.Write(new Span<byte>(byteArray, 16));
                    binaryWriter.Write((byte)RequestType);
                    switch (RequestType)
                    {
                        case ClientMessageType.RequestFullPicture:
                            break;
                    }
                }
            }
        }

        public void Deserialize(byte[] messageInBytes)
        {
            if (messageInBytes.Length == 0)
                return;

            using (var memoryStream = new MemoryStream(messageInBytes))
            using (var binaryReader = new BinaryReader(memoryStream))
            {
                Decode(binaryReader);
            }
        }

        public void Decode(BinaryReader binaryReader)
        {
            unsafe
            {
                {
                    Span<byte> buffer = stackalloc byte[16];
                    binaryReader.Read(buffer);

                    ClientGuid = new Guid(buffer);

                    RequestType = (ClientMessageType)binaryReader.ReadByte();

                    switch (RequestType)
                    {
                        case ClientMessageType.RequestFullPicture:
                            break;
                    }
                }
            }
        }
    }
}

