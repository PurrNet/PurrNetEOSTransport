using System;
using System.Collections.Generic;
using PurrNet.Transports;
using UnityEngine;
#if EOS_SDK
using Epic.OnlineServices;
using Epic.OnlineServices.P2P;
using PlayEveryWare.EpicOnlineServices;
#endif

namespace PurrNet.EOSTransport
{
    [DefaultExecutionOrder(-100)]
    public class EOSTransport : GenericTransport, ITransport
    {
        [Header("EOS Settings")]
        [SerializeField] string _socketName = "PurrNetEOS";
        [SerializeField] string _remoteProductUserId;

        [Header("Timeout")]
        [SerializeField, Tooltip("Seconds without any received packet before the connection is considered timed out and dropped with DisconnectReason.Timeout.")]
        [Min(0.5f)] float _connectionTimeout = 5f;

        [SerializeField, Tooltip("Seconds between keep-alive packets sent to each peer. Should be well under Connection Timeout (e.g. 1/5).")]
        [Min(0.1f)] float _heartbeatInterval = 1f;

        [Header("Logging")]
        [SerializeField, Tooltip("Minimum log level emitted by this transport. None silences all output; Info is the most verbose.")]
        EOSLogLevel _logLevel = EOSLogLevel.Warning;

        public EOSLogLevel logLevel
        {
            get => _logLevel;
            set => _logLevel = value;
        }

        internal void LogInfo(string msg)
        {
            if (_logLevel >= EOSLogLevel.Info)
                Debug.Log(msg);
        }

        internal void LogWarning(string msg)
        {
            if (_logLevel >= EOSLogLevel.Warning)
                Debug.LogWarning(msg);
        }

        internal void LogError(string msg)
        {
            if (_logLevel >= EOSLogLevel.Error)
                Debug.LogError(msg);
        }

        public string socketName
        {
            get => _socketName;
            set => _socketName = value;
        }

        public string remoteProductUserId
        {
            get => _remoteProductUserId;
            set => _remoteProductUserId = value;
        }

        public float connectionTimeout
        {
            get => _connectionTimeout;
            set => _connectionTimeout = value;
        }

        public float heartbeatInterval
        {
            get => _heartbeatInterval;
            set => _heartbeatInterval = value;
        }

        public int GetMTU(Connection target, Channel channel, bool asServer)
        {
            return channel switch
            {
                Channel.Unreliable or Channel.UnreliableSequenced =>
                    EOSPeer.EOS_MAX_PACKET - FragmentationLayer.UNFRAGMENTED_OVERHEAD,
                Channel.ReliableOrdered or Channel.ReliableUnordered =>
                    FragmentationLayer.GetMaxMessageSize(EOSPeer.EOS_MAX_PACKET),
                _ => EOSPeer.EOS_MAX_PACKET - FragmentationLayer.UNFRAGMENTED_OVERHEAD
            };
        }

#if EOS_SDK
        public override bool isSupported => true;
#else
        public override bool isSupported => false;
#endif

        public override ITransport transport => this;

        readonly List<Connection> _connections = new();

        struct LoopbackPacket
        {
            public Connection connection;
            public byte[] data;
            public int length;
            public bool asServer;
        }

        readonly Queue<LoopbackPacket> _loopbackQueue = new();

        public IReadOnlyList<Connection> connections => _connections;

        ConnectionState _listenerState = ConnectionState.Disconnected;

        public ConnectionState listenerState
        {
            get => _listenerState;
            private set
            {
                if (_listenerState == value)
                    return;
                _listenerState = value;
                onConnectionState?.Invoke(_listenerState, true);
            }
        }

        ConnectionState _clientState = ConnectionState.Disconnected;

        public ConnectionState clientState
        {
            get => _clientState;
            private set
            {
                if (_clientState == value)
                    return;
                _clientState = value;
                onConnectionState?.Invoke(_clientState, false);
            }
        }

        public event OnConnected onConnected;
        public event OnDisconnected onDisconnected;
        public event OnDataReceived onDataReceived;
        public event OnDataSent onDataSent;
        public event OnConnectionState onConnectionState;

        EOSServer _server;
        EOSClient _client;

        bool _isHostLoopback;
        const int LOOPBACK_CONNECTION_ID = 0;

        protected override void StartClientInternal()
        {
            if (_server != null && listenerState == ConnectionState.Connected)
            {
                StartHostLoopback();
                return;
            }

            Connect(_remoteProductUserId, 0);
        }

        protected override void StartServerInternal()
        {
            Listen(0);
        }

        void StartHostLoopback()
        {
            _isHostLoopback = true;

            _connections.Add(new Connection(LOOPBACK_CONNECTION_ID));
            onConnected?.Invoke(new Connection(LOOPBACK_CONNECTION_ID), true);

            clientState = ConnectionState.Connected;
            onConnected?.Invoke(new Connection(0), false);
        }

        public void Listen(ushort port)
        {
            if (_server != null)
                StopListening();

            listenerState = ConnectionState.Connecting;

#if EOS_SDK
            ConfigurePacketQueue();
#endif

            _server = new EOSServer();
            _server.Initialize(this);

            if (_server.Listen())
            {
                listenerState = ConnectionState.Connected;
            }
            else
            {
                listenerState = ConnectionState.Disconnecting;
                listenerState = ConnectionState.Disconnected;
            }

            _server.onDataReceived += OnServerData;
            _server.onRemoteConnected += OnRemoteConnected;
            _server.onRemoteDisconnected += OnRemoteDisconnected;
        }

        void OnRemoteConnected(int connectionId)
        {
            _connections.Add(new Connection(connectionId));
            onConnected?.Invoke(new Connection(connectionId), true);
        }

        void OnRemoteDisconnected(int connectionId, DisconnectReason reason)
        {
            _connections.Remove(new Connection(connectionId));
            onDisconnected?.Invoke(new Connection(connectionId), reason, true);
        }

        void OnServerData(int connectionId, ByteData data)
        {
            onDataReceived?.Invoke(new Connection(connectionId), data, true);
        }

        public void StopListening()
        {
            if (listenerState != ConnectionState.Disconnected)
                listenerState = ConnectionState.Disconnecting;

            _server?.Stop();
            listenerState = ConnectionState.Disconnected;
            _server = null;
            _isHostLoopback = false;
        }

        public void Connect(string address, ushort port)
        {
            if (_client != null)
                Disconnect();

#if EOS_SDK
            ConfigurePacketQueue();
#endif

            _client = new EOSClient();
            _client.Initialize(this);
            _client.onConnectionState += OnClientStateChanged;
            _client.onDataReceived += OnClientDataReceived;

            _client.Connect(address);
        }

        void OnClientDataReceived(ByteData data)
        {
            onDataReceived?.Invoke(new Connection(-1), data, false);
        }

        void OnClientStateChanged(ConnectionState state, DisconnectReason reason)
        {
            clientState = state;

            if (state == ConnectionState.Connected)
                onConnected?.Invoke(new Connection(0), false);

            if (state == ConnectionState.Disconnected)
                onDisconnected?.Invoke(new Connection(0), reason, false);
        }

        public void Disconnect()
        {
            if (_isHostLoopback)
            {
                _connections.Remove(new Connection(LOOPBACK_CONNECTION_ID));
                onDisconnected?.Invoke(new Connection(LOOPBACK_CONNECTION_ID), DisconnectReason.ClientRequest, true);
                onDisconnected?.Invoke(new Connection(0), DisconnectReason.ClientRequest, false);
                clientState = ConnectionState.Disconnected;
                _isHostLoopback = false;
                return;
            }

            if (_client == null)
                return;

            _client.Stop();
            _client = null;
        }

        public void RaiseDataReceived(Connection conn, ByteData data, bool asServer)
        {
            onDataReceived?.Invoke(conn, data, asServer);
        }

        public void RaiseDataSent(Connection conn, ByteData data, bool asServer)
        {
            onDataSent?.Invoke(conn, data, asServer);
        }

        public void SendToClient(Connection target, ByteData data, Channel method = Channel.ReliableOrdered)
        {
            if (listenerState is not ConnectionState.Connected)
                return;

            if (!target.isValid)
                return;

            if (_isHostLoopback && target.connectionId == LOOPBACK_CONNECTION_ID)
            {
                var copy = new byte[data.length];
                Buffer.BlockCopy(data.data, data.offset, copy, 0, data.length);
                _loopbackQueue.Enqueue(new LoopbackPacket
                {
                    connection = new Connection(-1),
                    data = copy,
                    length = data.length,
                    asServer = false
                });
                RaiseDataSent(target, data, true);
                return;
            }

            _server.SendToConnection(target.connectionId, data, method);
            RaiseDataSent(target, data, true);
        }

        public void SendToServer(ByteData data, Channel method = Channel.ReliableOrdered)
        {
            if (_isHostLoopback)
            {
                var copy = new byte[data.length];
                Buffer.BlockCopy(data.data, data.offset, copy, 0, data.length);
                _loopbackQueue.Enqueue(new LoopbackPacket
                {
                    connection = new Connection(LOOPBACK_CONNECTION_ID),
                    data = copy,
                    length = data.length,
                    asServer = true
                });
                RaiseDataSent(default, data, false);
                return;
            }

            _client.Send(data, method);
            RaiseDataSent(default, data, false);
        }

        public void CloseConnection(Connection conn)
        {
            if (_isHostLoopback && conn.connectionId == LOOPBACK_CONNECTION_ID)
            {
                Disconnect();
                return;
            }

            _server.CloseConnection(conn.connectionId);
        }

        public void ReceiveMessages(float delta)
        {
            int loopbackCount = _loopbackQueue.Count;
            for (int i = 0; i < loopbackCount; i++)
            {
                var packet = _loopbackQueue.Dequeue();
                var byteData = new ByteData(packet.data, 0, packet.length);
                onDataReceived?.Invoke(packet.connection, byteData, packet.asServer);
            }

            _server?.ReceiveMessages();
            _client?.ReceiveMessages();
        }

        public void SendMessages(float delta)
        {
            _server?.SendMessages();
            _client?.SendMessages();
        }

#if EOS_SDK
        void ConfigurePacketQueue()
        {
            var p2p = EOSManager.Instance?.GetEOSPlatformInterface()?.GetP2PInterface();
            if (p2p == null) return;

            var options = new SetPacketQueueSizeOptions
            {
                IncomingPacketQueueMaxSizeBytes = 4 * 1024 * 1024,
                OutgoingPacketQueueMaxSizeBytes = 4 * 1024 * 1024
            };

            var result = p2p.SetPacketQueueSize(ref options);
            if (result != Result.Success)
                LogWarning($"[EOSTransport] SetPacketQueueSize returned: {result}");
        }
#endif
    }

    public enum EOSLogLevel
    {
        None = 0,
        Error = 1,
        Warning = 2,
        Info = 3,
    }
}
