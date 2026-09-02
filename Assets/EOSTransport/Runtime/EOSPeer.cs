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
        public const byte PING_BYTE = 2;
        public const byte PONG_BYTE = 3;
        public const byte CONTROL_CHANNEL = 255;

        // Backpressure ceiling for the managed overflow queues. When EOS reports
        // LimitExceeded we buffer sends, but a peer that stays this far behind is
        // unrecoverable — unbounded buffering turns into a GC death spiral on the
        // sender (observed in the wild: ~35k queued buffers / ~1.8 GB at 1 fps).
        // Past the cap, Send reports failure so the owner disconnects the peer.
        public const long MAX_QUEUED_BYTES = 32L * 1024 * 1024;

        static readonly byte[] HEARTBEAT_PAYLOAD = { HEARTBEAT_BYTE };

        public readonly FragmentationLayer fragLayer = new();

        public float lastReceivedTime;
        public float lastHeartbeatSentTime;
        public int roundTripTime = -1;

        readonly byte[] _pingBuffer = new byte[5];
        readonly byte[] _pongBuffer = new byte[5];

        readonly Queue<QueuedFragment> _fragmentQueue = new();
        readonly Queue<QueuedMessage> _pendingMessages = new();

        bool _queueing;
        Channel _currentChannel;
        bool _sendFailed;
        long _queuedBytes;
        bool _overflowReported;

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

        // EOS defaults its native P2P queues to 64 KB per direction — a single reliable
        // message bigger than that (e.g. a join-time state snapshot) instantly hits
        // LimitExceeded and pushes buffering into the managed overflow queues. Larger
        // native queues let EOS spool bursts itself.
        public static void ConfigurePacketQueueSize(P2PInterface p2p, EOSTransport transport)
        {
            var options = new SetPacketQueueSizeOptions
            {
                IncomingPacketQueueMaxSizeBytes = 4UL * 1024 * 1024,
                OutgoingPacketQueueMaxSizeBytes = 2UL * 1024 * 1024
            };
            var result = p2p.SetPacketQueueSize(ref options);
            if (result != Result.Success)
                transport.LogWarning($"[EOSPeer] SetPacketQueueSize failed: {result}");
        }

        public Result SendPing()
        {
            uint now = (uint)(UnityEngine.Time.unscaledTimeAsDouble * 1000);
            _pingBuffer[0] = PING_BYTE;
            WriteUInt(_pingBuffer, 1, now);
            return SendControl(_pingBuffer);
        }

        public bool HandleControl(byte channel, ByteData data)
        {
            if (channel != CONTROL_CHANNEL)
                return false;

            if (data.length != 5)
                return true;

            switch (data.data[data.offset])
            {
                case PING_BYTE:
                    _pongBuffer[0] = PONG_BYTE;
                    Buffer.BlockCopy(data.data, data.offset + 1, _pongBuffer, 1, 4);
                    SendControl(_pongBuffer);
                    break;
                case PONG_BYTE:
                    uint now = (uint)(UnityEngine.Time.unscaledTimeAsDouble * 1000);
                    roundTripTime = (int)(now - ReadUInt(data.data, data.offset + 1));
                    break;
            }

            return true;
        }

        Result SendControl(byte[] payload)
        {
            var options = new SendPacketOptions
            {
                LocalUserId = _localUserId,
                RemoteUserId = _remoteUserId,
                SocketId = _socketId,
                Channel = CONTROL_CHANNEL,
                Data = new ArraySegment<byte>(payload),
                AllowDelayedDelivery = true,
                Reliability = PacketReliability.UnreliableUnordered
            };
            return _p2p.SendPacket(ref options);
        }

        static void WriteUInt(byte[] buffer, int offset, uint value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }

        static uint ReadUInt(byte[] buffer, int offset)
        {
            return buffer[offset]
                   | (uint)buffer[offset + 1] << 8
                   | (uint)buffer[offset + 2] << 16
                   | (uint)buffer[offset + 3] << 24;
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
                if (IsOverflowing(data.length))
                    return false;

                var copy = new byte[data.length];
                Buffer.BlockCopy(data.data, data.offset, copy, 0, data.length);
                _pendingMessages.Enqueue(new QueuedMessage
                {
                    data = copy,
                    length = data.length,
                    channel = channel
                });
                _queuedBytes += data.length;
                return true;
            }

            _currentChannel = channel;
            fragLayer.Send(data, EOS_MAX_PACKET, _sendDelegate);
            return !_sendFailed;
        }

        bool IsOverflowing(int incomingLength)
        {
            if (_queuedBytes + incomingLength <= MAX_QUEUED_BYTES)
                return false;

            if (!_overflowReported)
            {
                _overflowReported = true;
                _transport.LogError(
                    $"[EOSPeer] Send backlog exceeded {MAX_QUEUED_BYTES / (1024 * 1024)} MB " +
                    $"({_queuedBytes / (1024 * 1024)} MB queued) — peer cannot keep up, disconnecting.");
            }
            return true;
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
            if (IsOverflowing(fragment.length))
            {
                _sendFailed = true;
                return;
            }

            var copy = new byte[fragment.length];
            Buffer.BlockCopy(fragment.data, fragment.offset, copy, 0, fragment.length);
            _fragmentQueue.Enqueue(new QueuedFragment
            {
                data = copy,
                length = fragment.length,
                channel = channel
            });
            _queuedBytes += fragment.length;
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
                _queuedBytes -= frag.length;

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
                _queuedBytes -= msg.length;

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
            _queuedBytes = 0;
            _overflowReported = false;
        }
    }
}
