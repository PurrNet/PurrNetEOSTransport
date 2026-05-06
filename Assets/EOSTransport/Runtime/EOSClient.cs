using System;
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
        public event Action<ConnectionState> onConnectionState;

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
            set
            {
                if (_state == value)
                    return;
                _state = value;
                onConnectionState?.Invoke(_state);
            }
        }

        public ConnectionState connectionState => _state;

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
                    UnityEngine.Debug.LogError("[EOSClient] P2P interface or local user not available");
                    State = ConnectionState.Disconnected;
                    return;
                }

                var remoteUserId = ProductUserId.FromString(remoteProductUserId);
                if (remoteUserId == null)
                {
                    UnityEngine.Debug.LogError("[EOSClient] Invalid remote ProductUserId");
                    State = ConnectionState.Disconnected;
                    return;
                }

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
                    UnityEngine.Debug.LogError($"[EOSClient] AcceptConnection failed: {acceptResult}");
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

                _serverPeer = new EOSPeer(_p2p, _localUserId, remoteUserId, _transport.socketName);

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
                    UnityEngine.Debug.LogError($"[EOSClient] Failed to send handshake: {sendResult}");
                    State = ConnectionState.Disconnected;
                }
                else
                {
                    UnityEngine.Debug.Log("[EOSClient] Handshake sent, waiting for connection...");
                }
            }
            catch (Exception e)
            {
                UnityEngine.Debug.LogError($"[EOSClient] Failed to connect: {e}");
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
                UnityEngine.Debug.LogError("[EOSClient] Send failed, disconnecting");
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

                var buffer = new byte[packetSize];
                var receiveOptions = new ReceivePacketOptions
                {
                    LocalUserId = _localUserId,
                    MaxDataSizeBytes = packetSize
                };

                ProductUserId remoteUserId = null;
                var socketId = new SocketId();
                byte eosChannel = 0;

                var result = _p2p.ReceivePacket(
                    ref receiveOptions,
                    ref remoteUserId,
                    ref socketId,
                    out eosChannel,
                    new ArraySegment<byte>(buffer),
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

                if (_serverPeer.fragLayer.Receive(rawData, out var assembled))
                {
                    if (_state == ConnectionState.Connecting)
                    {
                        UnityEngine.Debug.Log("[EOSClient] Connection established with server (first data)");
                        State = ConnectionState.Connected;
                    }

                    onDataReceived?.Invoke(assembled);
                }
            }
#endif
        }

        public void SendMessages()
        {
#if EOS_SDK
            if (_serverPeer == null)
                return;

            _serverPeer.FlushQueue();

            float now = UnityEngine.Time.unscaledTime;
            if (now - _lastCleanupTime > 5f)
            {
                _lastCleanupTime = now;
                _serverPeer.fragLayer.CleanupStale(30000);
            }
#endif
        }

        public void Stop()
        {
#if EOS_SDK
            if (_state == ConnectionState.Disconnected)
                return;

            State = ConnectionState.Disconnecting;

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

            State = ConnectionState.Disconnected;
#endif
        }

#if EOS_SDK
        void OnConnectionEstablished(ref OnPeerConnectionEstablishedInfo info)
        {
            if (info.RemoteUserId.ToString() != _remoteProductUserId)
                return;

            if (_state == ConnectionState.Connecting)
            {
                UnityEngine.Debug.Log("[EOSClient] Connection established with server (EOS notification)");
                State = ConnectionState.Connected;
            }
        }

        void OnConnectionClosed(ref OnRemoteConnectionClosedInfo info)
        {
            if (info.RemoteUserId.ToString() != _remoteProductUserId)
                return;

            UnityEngine.Debug.Log($"[EOSClient] Connection closed (reason={info.Reason})");

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
            catch (Exception e) { UnityEngine.Debug.LogWarning($"[EOSClient] RemoveNotifyPeerConnectionEstablished failed: {e.Message}"); }
            _notifyEstablishedHandle = Common.INVALID_NOTIFICATIONID;
        }

        void SafeRemoveClosed(P2PInterface p2p)
        {
            if (_notifyClosedHandle == Common.INVALID_NOTIFICATIONID)
                return;
            try { p2p.RemoveNotifyPeerConnectionClosed(_notifyClosedHandle); }
            catch (Exception e) { UnityEngine.Debug.LogWarning($"[EOSClient] RemoveNotifyPeerConnectionClosed failed: {e.Message}"); }
            _notifyClosedHandle = Common.INVALID_NOTIFICATIONID;
        }
#endif
    }
}
