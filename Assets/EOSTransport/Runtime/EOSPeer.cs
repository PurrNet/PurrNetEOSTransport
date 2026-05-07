using System;
using System.Collections.Generic;
using PurrNet.Transports;
#if EOS_SDK
using Epic.OnlineServices;
using Epic.OnlineServices.P2P;
#endif

namespace PurrNet.EOSTransport
{
    public class EOSPeer : IDisposable
    {
        public const int EOS_MAX_PACKET = 1170;
        public const byte HEARTBEAT_BYTE = 1;

        static readonly byte[] HEARTBEAT_PAYLOAD = { HEARTBEAT_BYTE };

        public readonly FragmentationLayer fragLayer = new();

        public float lastReceivedTime;
        public float lastHeartbeatSentTime;

        readonly Queue<QueuedFragment> _fragmentQueue = new();
        readonly Queue<QueuedMessage> _pendingMessages = new();

        bool _queueing;
        Channel _currentChannel;
        bool _sendFailed;

        struct QueuedFragment
        {
            public byte[] data;
            public int length;
            public Channel channel;
        }

        struct QueuedMessage
        {
            public byte[] data;
            public int length;
            public Channel channel;
        }

#if EOS_SDK
        readonly EOSTransport _transport;
        readonly ProductUserId _localUserId;
        readonly ProductUserId _remoteUserId;
        readonly SocketId _socketId;
        readonly P2PInterface _p2p;
        readonly Action<ByteData> _sendDelegate;

        public EOSPeer(EOSTransport transport, P2PInterface p2p, ProductUserId localUserId, ProductUserId remoteUserId, string socketName)
        {
            _transport = transport;
            _p2p = p2p;
            _localUserId = localUserId;
            _remoteUserId = remoteUserId;
            _socketId = new SocketId { SocketName = socketName };
            _sendDelegate = OnSendFragment;
            lastReceivedTime = UnityEngine.Time.unscaledTime;
            lastHeartbeatSentTime = 0f;
        }

        public static bool IsHeartbeat(ByteData data)
        {
            return data.length == 1 && data.data[data.offset] == HEARTBEAT_BYTE;
        }

        public Result SendHeartbeat()
        {
            var options = new SendPacketOptions
            {
                LocalUserId = _localUserId,
                RemoteUserId = _remoteUserId,
                SocketId = _socketId,
                Channel = (byte)Channel.Unreliable,
                Data = new ArraySegment<byte>(HEARTBEAT_PAYLOAD),
                AllowDelayedDelivery = true,
                Reliability = PacketReliability.UnreliableUnordered
            };
            return _p2p.SendPacket(ref options);
        }

        public bool Send(ByteData data, Channel channel)
        {
            _sendFailed = false;

            if (_queueing)
            {
                var copy = new byte[data.length];
                Buffer.BlockCopy(data.data, data.offset, copy, 0, data.length);
                _pendingMessages.Enqueue(new QueuedMessage
                {
                    data = copy,
                    length = data.length,
                    channel = channel
                });
                return true;
            }

            _currentChannel = channel;
            fragLayer.Send(data, EOS_MAX_PACKET, _sendDelegate);
            return !_sendFailed;
        }

        void OnSendFragment(ByteData fragment)
        {
            if (_queueing)
            {
                EnqueueFragment(fragment, _currentChannel);
                return;
            }

            var result = SendRawEOS(fragment, _currentChannel);

            if (result == Result.LimitExceeded)
            {
                _queueing = true;
                EnqueueFragment(fragment, _currentChannel);
            }
            else if (result != Result.Success)
            {
                _sendFailed = true;
                _transport.LogError($"[EOSPeer] SendPacket failed: {result}");
            }
        }

        void EnqueueFragment(ByteData fragment, Channel channel)
        {
            var copy = new byte[fragment.length];
            Buffer.BlockCopy(fragment.data, fragment.offset, copy, 0, fragment.length);
            _fragmentQueue.Enqueue(new QueuedFragment
            {
                data = copy,
                length = fragment.length,
                channel = channel
            });
        }

        Result SendRawEOS(ByteData data, Channel channel)
        {
            var options = new SendPacketOptions
            {
                LocalUserId = _localUserId,
                RemoteUserId = _remoteUserId,
                SocketId = _socketId,
                Channel = (byte)channel,
                Data = new ArraySegment<byte>(data.data, data.offset, data.length),
                AllowDelayedDelivery = true,
                Reliability = GetReliability(channel)
            };

            return _p2p.SendPacket(ref options);
        }

        Result SendRawEOS(byte[] data, int length, Channel channel)
        {
            var options = new SendPacketOptions
            {
                LocalUserId = _localUserId,
                RemoteUserId = _remoteUserId,
                SocketId = _socketId,
                Channel = (byte)channel,
                Data = new ArraySegment<byte>(data, 0, length),
                AllowDelayedDelivery = true,
                Reliability = GetReliability(channel)
            };

            return _p2p.SendPacket(ref options);
        }

        public void FlushQueue()
        {
            while (_fragmentQueue.Count > 0)
            {
                var frag = _fragmentQueue.Peek();
                var result = SendRawEOS(frag.data, frag.length, frag.channel);

                if (result == Result.LimitExceeded)
                    return;

                _fragmentQueue.Dequeue();

                if (result != Result.Success)
                    _transport.LogError($"[EOSPeer] Queued fragment send failed: {result}");
            }

            while (_pendingMessages.Count > 0)
            {
                var msg = _pendingMessages.Peek();
                _currentChannel = msg.channel;
                _queueing = false;
                _sendFailed = false;

                fragLayer.Send(new ByteData(msg.data, 0, msg.length), EOS_MAX_PACKET, _sendDelegate);
                _pendingMessages.Dequeue();

                if (_queueing)
                    return;
            }

            _queueing = false;
        }

        static PacketReliability GetReliability(Channel channel)
        {
            return channel switch
            {
                Channel.ReliableOrdered => PacketReliability.ReliableOrdered,
                Channel.ReliableUnordered => PacketReliability.ReliableUnordered,
                Channel.UnreliableSequenced => PacketReliability.UnreliableUnordered,
                Channel.Unreliable => PacketReliability.UnreliableUnordered,
                _ => PacketReliability.ReliableOrdered
            };
        }
#else
        public EOSPeer() { }
        public bool Send(ByteData data, Channel channel) => false;
        public void FlushQueue() { }
        public static bool IsHeartbeat(ByteData data) => false;
#endif

        public void Dispose()
        {
            fragLayer.Dispose();
            _fragmentQueue.Clear();
            _pendingMessages.Clear();
            _queueing = false;
        }
    }
}
