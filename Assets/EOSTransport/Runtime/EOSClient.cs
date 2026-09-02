using System;
using System.Buffers;
using PurrNet.Transports;
#if EOS_SDK
using Epic.OnlineServices;
using Epic.OnlineServices.P2P;
using PlayEveryWare.EpicOnlineServices;
#endif

namespace PurrNet.EOSTransport
{
    public class EOSClient
    {
        EOSTransport _transport;

        EOSPeer _serverPeer;
        string _remoteProductUserId;
        ConnectionState _state = ConnectionState.Disconnected;

        float _lastCleanupTime;

        public event Action<ByteData> onDataReceived;
        public event Action<ConnectionState, DisconnectReason> onConnectionState;

        static readonly byte[] HANDSHAKE = { 0 };

#if EOS_SDK
        P2PInterface _p2p;
        ProductUserId _localUserId;
        SocketId _socketId;

        ulong _notifyEstablishedHandle;
        ulong _notifyClosedHandle;
#endif

        ConnectionState State
        {
            get => _state;
            set => SetState(value, DisconnectReason.ClientRequest);
        }

        void SetState(ConnectionState newState, DisconnectReason reason)
        {
            if (_state == newState)
                return;
            _state = newState;
            onConnectionState?.Invoke(_state, reason);
        }

        public ConnectionState connectionState => _state;

        public int roundTripTime => _serverPeer?.roundTripTime ?? -1;

        public void Initialize(EOSTransport transport)
        {
            _transport = transport;
        }

        public void Connect(string remoteProductUserId)
        {
#if EOS_SDK
            try
            {
                _remoteProductUserId = remoteProductUserId;
                State = ConnectionState.Connecting;

                _p2p = EOSManager.Instance?.GetEOSPlatformInterface()?.GetP2PInterface();
                var connectInterface = EOSManager.Instance?.GetEOSPlatformInterface()?.GetConnectInterface();
                _localUserId = connectInterface?.GetLoggedInUserByIndex(0);

                if (_p2p == null || _localUserId == null)
                {
                    _transport.LogError("[EOSClient] P2P interface or local user not available");
                    State = ConnectionState.Disconnected;
                    return;
                }

                var remoteUserId = ProductUserId.FromString(remoteProductUserId);
                if (remoteUserId == null)
                {
                    _transport.LogError("[EOSClient] Invalid remote ProductUserId");
                    State = ConnectionState.Disconnected;
                    return;
                }

                EOSPeer.ConfigurePacketQueueSize(_p2p, _transport);

                _socketId = new SocketId { SocketName = _transport.socketName };

                var acceptOptions = new AcceptConnectionOptions
                {
                    LocalUserId = _localUserId,
                    RemoteUserId = remoteUserId,
                    SocketId = _socketId
                };

                var acceptResult = _p2p.AcceptConnection(ref acceptOptions);
                if (acceptResult != Result.Success)
                {
                    _transport.LogError($"[EOSClient] AcceptConnection failed: {acceptResult}");
                    State = ConnectionState.Disconnected;
                    return;
                }

                var establishedOptions = new AddNotifyPeerConnectionEstablishedOptions
                {
                    LocalUserId = _localUserId,
                    SocketId = _socketId
                };
                _notifyEstablishedHandle = _p2p.AddNotifyPeerConnectionEstablished(ref establishedOptions, null, OnConnectionEstablished);

                var closedOptions = new AddNotifyPeerConnectionClosedOptions
                {
                    LocalUserId = _localUserId,
                    SocketId = _socketId
                };
                _notifyClosedHandle = _p2p.AddNotifyPeerConnectionClosed(ref closedOptions, null, OnConnectionClosed);

                _serverPeer = new EOSPeer(_transport, _p2p, _localUserId, remoteUserId, _transport.socketName);

                var sendOptions = new SendPacketOptions
                {
                    LocalUserId = _localUserId,
                    RemoteUserId = remoteUserId,
                    SocketId = _socketId,
                    Channel = (byte)Channel.ReliableOrdered,
                    Data = new ArraySegment<byte>(HANDSHAKE),
                    AllowDelayedDelivery = true,
                    Reliability = PacketReliability.ReliableOrdered
                };

                var sendResult = _p2p.SendPacket(ref sendOptions);
                if (sendResult != Result.Success)
                {
                    _transport.LogError($"[EOSClient] Failed to send handshake: {sendResult}");
                    State = ConnectionState.Disconnected;
                }
                else
                {
                    _transport.LogInfo("[EOSClient] Handshake sent, waiting for connection...");
                }
            }
            catch (Exception e)
            {
                _transport.LogError($"[EOSClient] Failed to connect: {e}");
                State = ConnectionState.Disconnected;
            }
#endif
        }

        public void Send(ByteData data, Channel channel)
        {
#if EOS_SDK
            if (_state != ConnectionState.Connected || _serverPeer == null)
                return;

            if (!_serverPeer.Send(data, channel))
            {
                _transport.LogError("[EOSClient] Send failed, disconnecting");
                Stop();
            }
#endif
        }

        public void ReceiveMessages()
        {
#if EOS_SDK
            if (_p2p == null || _localUserId == null)
                return;

            if (_state == ConnectionState.Disconnected)
                return;

            if (EOSManager.Instance == null || !EOSManager.Instance.HasLoggedInWithConnect())
                return;

            int maxReads = 2048;
            for (int i = 0; i < maxReads; i++)
            {
                var getSizeOptions = new GetNextReceivedPacketSizeOptions
                {
                    LocalUserId = _localUserId
                };

                if (_p2p.GetNextReceivedPacketSize(ref getSizeOptions, out var packetSize) != Result.Success)
                    break;

                int size = (int)packetSize;
                var buffer = ArrayPool<byte>.Shared.Rent(size);
                try
                {
                    var receiveOptions = new ReceivePacketOptions
                    {
                        LocalUserId = _localUserId,
                        MaxDataSizeBytes = packetSize
                    };

                    ProductUserId remoteUserId = null;
                    var socketId = new SocketId();

                    var result = _p2p.ReceivePacket(
                        ref receiveOptions,
                        ref remoteUserId,
                        ref socketId,
                        out var packetChannel,
                        new ArraySegment<byte>(buffer, 0, size),
                        out var bytesWritten);

                    if (result != Result.Success)
                        break;

                    if (socketId.SocketName != _transport.socketName)
                        continue;

                    if (remoteUserId.ToString() != _remoteProductUserId)
                        continue;

                    if (_serverPeer == null)
                        continue;

                    var rawData = new ByteData(buffer, 0, (int)bytesWritten);

                    _serverPeer.lastReceivedTime = UnityEngine.Time.unscaledTime;

                    if (_serverPeer.HandleControl(packetChannel, rawData))
                    {
                        if (_state == ConnectionState.Connecting)
                            State = ConnectionState.Connected;
                        continue;
                    }

                    if (EOSPeer.IsHeartbeat(rawData))
                    {
                        if (_state == ConnectionState.Connecting)
                        {
                            _transport.LogInfo("[EOSClient] Connection established with server (heartbeat)");
                            State = ConnectionState.Connected;
                        }
                        continue;
                    }

                    if (_serverPeer.fragLayer.Receive(rawData, out var assembled))
                    {
                        if (_state == ConnectionState.Connecting)
                        {
                            _transport.LogInfo("[EOSClient] Connection established with server (first data)");
                            State = ConnectionState.Connected;
                        }

                        onDataReceived?.Invoke(assembled);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
#endif
        }

        public void SendMessages()
        {
#if EOS_SDK
            if (_serverPeer == null)
                return;

            float now = UnityEngine.Time.unscaledTime;

            if (_state == ConnectionState.Connecting || _state == ConnectionState.Connected)
            {
                if (now - _serverPeer.lastReceivedTime > _transport.connectionTimeout)
                {
                    _transport.LogWarning($"[EOSClient] Connection timed out (no packet for >{_transport.connectionTimeout}s)");
                    StopWithReason(DisconnectReason.Timeout);
                    return;
                }

                if (_state == ConnectionState.Connected &&
                    now - _serverPeer.lastHeartbeatSentTime >= _transport.heartbeatInterval)
                {
                    if (_serverPeer.SendHeartbeat() == Result.Success)
                    {
                        _serverPeer.lastHeartbeatSentTime = now;
                        _serverPeer.SendPing();
                    }
                }
            }

            _serverPeer.FlushQueue();

            if (now - _lastCleanupTime > 5f)
            {
                _lastCleanupTime = now;
                _serverPeer.fragLayer.CleanupStale(30000);
            }
#endif
        }

        public void Stop()
        {
            StopWithReason(DisconnectReason.ClientRequest);
        }

        public void StopWithReason(DisconnectReason reason)
        {
#if EOS_SDK
            if (_state == ConnectionState.Disconnected)
                return;

            SetState(ConnectionState.Disconnecting, reason);

            var platform = EOSManager.Instance?.GetEOSPlatformInterface();
            var p2p = platform?.GetP2PInterface();

            if (p2p != null)
            {
                SafeRemoveEstablished(p2p);
                SafeRemoveClosed(p2p);

                var remoteUserId = ProductUserId.FromString(_remoteProductUserId);
                if (_localUserId != null && remoteUserId != null)
                {
                    var options = new CloseConnectionOptions
                    {
                        LocalUserId = _localUserId,
                        RemoteUserId = remoteUserId,
                        SocketId = _socketId
                    };
                    p2p.CloseConnection(ref options);
                }
            }

            _serverPeer?.Dispose();
            _serverPeer = null;

            SetState(ConnectionState.Disconnected, reason);
#endif
        }

#if EOS_SDK
        void OnConnectionEstablished(ref OnPeerConnectionEstablishedInfo info)
        {
            if (info.RemoteUserId.ToString() != _remoteProductUserId)
                return;

            if (_state == ConnectionState.Connecting)
            {
                _transport.LogInfo("[EOSClient] Connection established with server (EOS notification)");
                State = ConnectionState.Connected;
            }
        }

        void OnConnectionClosed(ref OnRemoteConnectionClosedInfo info)
        {
            if (info.RemoteUserId.ToString() != _remoteProductUserId)
                return;

            _transport.LogInfo($"[EOSClient] Connection closed (reason={info.Reason})");

            _serverPeer?.Dispose();
            _serverPeer = null;

            if (_state != ConnectionState.Disconnected)
            {
                var livePlatform = EOSManager.Instance?.GetEOSPlatformInterface();
                var liveP2p = livePlatform?.GetP2PInterface();
                if (liveP2p != null)
                {
                    SafeRemoveEstablished(liveP2p);
                    SafeRemoveClosed(liveP2p);
                }
                State = ConnectionState.Disconnected;
            }
        }

        void SafeRemoveEstablished(P2PInterface p2p)
        {
            if (_notifyEstablishedHandle == Common.INVALID_NOTIFICATIONID)
                return;
            try { p2p.RemoveNotifyPeerConnectionEstablished(_notifyEstablishedHandle); }
            catch (Exception e) { _transport.LogWarning($"[EOSClient] RemoveNotifyPeerConnectionEstablished failed: {e.Message}"); }
            _notifyEstablishedHandle = Common.INVALID_NOTIFICATIONID;
        }

        void SafeRemoveClosed(P2PInterface p2p)
        {
            if (_notifyClosedHandle == Common.INVALID_NOTIFICATIONID)
                return;
            try { p2p.RemoveNotifyPeerConnectionClosed(_notifyClosedHandle); }
            catch (Exception e) { _transport.LogWarning($"[EOSClient] RemoveNotifyPeerConnectionClosed failed: {e.Message}"); }
            _notifyClosedHandle = Common.INVALID_NOTIFICATIONID;
        }
#endif
    }
}
