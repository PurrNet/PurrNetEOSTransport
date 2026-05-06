using System;
using System.Collections.Generic;
using PurrNet.Transports;
#if EOS_SDK
using Epic.OnlineServices;
using Epic.OnlineServices.P2P;
using PlayEveryWare.EpicOnlineServices;
#endif

namespace PurrNet.EOSTransport
{
    public class EOSServer
    {
        EOSTransport _transport;

        readonly Dictionary<string, EOSPeer> _peers = new();
        readonly Dictionary<int, string> _connectionToUserId = new();
        readonly Dictionary<string, int> _userIdToConnection = new();
        int _nextConnectionId = 1;

        public event Action<int, ByteData> onDataReceived;
        public event Action<int> onRemoteConnected;
        public event Action<int> onRemoteDisconnected;

#if EOS_SDK
        P2PInterface _p2p;
        ProductUserId _localUserId;
        SocketId _socketId;

        ulong _notifyRequestHandle;
        ulong _notifyEstablishedHandle;
        ulong _notifyClosedHandle;

        float _lastCleanupTime;
#endif

        static bool IsHandshake(ByteData data)
        {
            return data.length == 1 && data.data[data.offset] == 0;
        }

        public void Initialize(EOSTransport transport)
        {
            _transport = transport;
        }

        public bool Listen()
        {
#if EOS_SDK
            try
            {
                _p2p = EOSManager.Instance?.GetEOSPlatformInterface()?.GetP2PInterface();
                var connectInterface = EOSManager.Instance?.GetEOSPlatformInterface()?.GetConnectInterface();
                _localUserId = connectInterface?.GetLoggedInUserByIndex(0);

                if (_p2p == null || _localUserId == null)
                {
                    UnityEngine.Debug.LogError("[EOSServer] P2P interface or local user not available");
                    return false;
                }

                _socketId = new SocketId { SocketName = _transport.socketName };

                var requestOptions = new AddNotifyPeerConnectionRequestOptions
                {
                    LocalUserId = _localUserId,
                    SocketId = _socketId
                };
                _notifyRequestHandle = _p2p.AddNotifyPeerConnectionRequest(ref requestOptions, null, OnConnectionRequest);

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

                return true;
            }
            catch (Exception e)
            {
                UnityEngine.Debug.LogError($"[EOSServer] Failed to start: {e}");
                return false;
            }
#else
            return false;
#endif
        }

        public void SendToConnection(int connectionId, ByteData data, Channel channel)
        {
#if EOS_SDK
            if (!_connectionToUserId.TryGetValue(connectionId, out var userId))
                return;

            if (!_peers.TryGetValue(userId, out var peer))
                return;

            if (!peer.Send(data, channel))
            {
                UnityEngine.Debug.LogError($"[EOSServer] Send failed for connection {connectionId}, disconnecting");
                CloseConnection(connectionId);
            }
#endif
        }

        public void CloseConnection(int connectionId)
        {
#if EOS_SDK
            if (!_connectionToUserId.TryGetValue(connectionId, out var userId))
                return;

            if (_peers.TryGetValue(userId, out var peer))
                peer.Dispose();

            var remoteUserId = ProductUserId.FromString(userId);
            if (remoteUserId != null && _p2p != null)
            {
                var options = new CloseConnectionOptions
                {
                    LocalUserId = _localUserId,
                    RemoteUserId = remoteUserId,
                    SocketId = _socketId
                };
                _p2p.CloseConnection(ref options);
            }

            _peers.Remove(userId);
            _connectionToUserId.Remove(connectionId);
            _userIdToConnection.Remove(userId);

            onRemoteDisconnected?.Invoke(connectionId);
#endif
        }

        public void ReceiveMessages()
        {
#if EOS_SDK
            if (_p2p == null || _localUserId == null)
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

                var rawData = new ByteData(buffer, 0, (int)bytesWritten);

                if (IsHandshake(rawData))
                    continue;

                var userIdStr = remoteUserId.ToString();
                if (!_peers.TryGetValue(userIdStr, out var peer))
                    continue;

                if (!_userIdToConnection.TryGetValue(userIdStr, out var connectionId))
                    continue;

                if (peer.fragLayer.Receive(rawData, out var assembled))
                    onDataReceived?.Invoke(connectionId, assembled);
            }
#endif
        }

        public void SendMessages()
        {
#if EOS_SDK
            float now = UnityEngine.Time.unscaledTime;
            bool doCleanup = now - _lastCleanupTime > 5f;
            if (doCleanup)
                _lastCleanupTime = now;

            foreach (var kvp in _peers)
            {
                kvp.Value.FlushQueue();
                if (doCleanup)
                    kvp.Value.fragLayer.CleanupStale(30000);
            }
#endif
        }

        public void Stop()
        {
#if EOS_SDK
            var platform = EOSManager.Instance?.GetEOSPlatformInterface();
            var p2p = platform?.GetP2PInterface();

            if (p2p != null)
            {
                SafeRemoveRequest(p2p);
                SafeRemoveEstablished(p2p);
                SafeRemoveClosed(p2p);

                var closeAllOptions = new CloseConnectionsOptions
                {
                    LocalUserId = _localUserId,
                    SocketId = _socketId
                };
                p2p.CloseConnections(ref closeAllOptions);
            }

            foreach (var kvp in _peers)
                kvp.Value.Dispose();

            _peers.Clear();
            _connectionToUserId.Clear();
            _userIdToConnection.Clear();
#endif
        }

#if EOS_SDK
        void OnConnectionRequest(ref OnIncomingConnectionRequestInfo info)
        {
            var acceptOptions = new AcceptConnectionOptions
            {
                LocalUserId = info.LocalUserId,
                RemoteUserId = info.RemoteUserId,
                SocketId = info.SocketId
            };

            var result = _p2p.AcceptConnection(ref acceptOptions);
            if (result != Result.Success)
                UnityEngine.Debug.LogError($"[EOSServer] AcceptConnection failed: {result}");
        }

        void OnConnectionEstablished(ref OnPeerConnectionEstablishedInfo info)
        {
            var userIdStr = info.RemoteUserId.ToString();

            if (_peers.ContainsKey(userIdStr))
                return;

            var peer = new EOSPeer(_p2p, _localUserId, info.RemoteUserId, _transport.socketName);
            int connectionId = _nextConnectionId++;

            _peers[userIdStr] = peer;
            _connectionToUserId[connectionId] = userIdStr;
            _userIdToConnection[userIdStr] = connectionId;

            UnityEngine.Debug.Log($"[EOSServer] Peer connected: {userIdStr} (id={connectionId})");
            onRemoteConnected?.Invoke(connectionId);
        }

        void OnConnectionClosed(ref OnRemoteConnectionClosedInfo info)
        {
            var userIdStr = info.RemoteUserId.ToString();

            if (!_userIdToConnection.TryGetValue(userIdStr, out var connectionId))
                return;

            if (_peers.TryGetValue(userIdStr, out var peer))
                peer.Dispose();

            _peers.Remove(userIdStr);
            _connectionToUserId.Remove(connectionId);
            _userIdToConnection.Remove(userIdStr);

            UnityEngine.Debug.Log($"[EOSServer] Peer disconnected: {userIdStr} (id={connectionId}, reason={info.Reason})");
            onRemoteDisconnected?.Invoke(connectionId);
        }

        void SafeRemoveRequest(P2PInterface p2p)
        {
            if (_notifyRequestHandle == Common.INVALID_NOTIFICATIONID)
                return;
            try { p2p.RemoveNotifyPeerConnectionRequest(_notifyRequestHandle); }
            catch (Exception e) { UnityEngine.Debug.LogWarning($"[EOSServer] RemoveNotifyPeerConnectionRequest failed: {e.Message}"); }
            _notifyRequestHandle = Common.INVALID_NOTIFICATIONID;
        }

        void SafeRemoveEstablished(P2PInterface p2p)
        {
            if (_notifyEstablishedHandle == Common.INVALID_NOTIFICATIONID)
                return;
            try { p2p.RemoveNotifyPeerConnectionEstablished(_notifyEstablishedHandle); }
            catch (Exception e) { UnityEngine.Debug.LogWarning($"[EOSServer] RemoveNotifyPeerConnectionEstablished failed: {e.Message}"); }
            _notifyEstablishedHandle = Common.INVALID_NOTIFICATIONID;
        }

        void SafeRemoveClosed(P2PInterface p2p)
        {
            if (_notifyClosedHandle == Common.INVALID_NOTIFICATIONID)
                return;
            try { p2p.RemoveNotifyPeerConnectionClosed(_notifyClosedHandle); }
            catch (Exception e) { UnityEngine.Debug.LogWarning($"[EOSServer] RemoveNotifyPeerConnectionClosed failed: {e.Message}"); }
            _notifyClosedHandle = Common.INVALID_NOTIFICATIONID;
        }
#endif
    }
}
