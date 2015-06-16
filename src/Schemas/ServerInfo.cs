// ---------------------------------------------------------------------
// <copyright file="ServerInfo.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System;

    using Bond;

    /// <summary>
    /// Details of MetricSystem server registered with aggregation server
    /// </summary>
    [Schema]
    public class ServerInfo : ICloneable, IComparable<ServerInfo>
    {
        private string hostname;
        private ushort port;

        public ServerInfo()
        {
            this.Hostname = string.Empty;
            this.Port = Protocol.DefaultServerPort;
            this.MachineFunction = string.Empty;
            this.Datacenter = string.Empty;
        }

        /// <summary>
        /// The hostname of the server.
        /// </summary>
        [Id(1), Required]
        public string Hostname
        {
            get { return this.hostname; }
            set { this.hostname = value ?? string.Empty; }
        }

        /// <summary>
        /// The port the server responds on for HTTP queries. Setting the port to '0' will use the value in
        /// <see cref="Protocol.DefaultServerPort"/>
        /// </summary>
        [Id(2), Required]
        public ushort Port
        {
            get { return this.port; }
            set
            {
                if (value == 0)
                {
                    // It would be nicer to throw here, but because of the way the Bond schema compiler works we
                    // can't do this.
                    value = Protocol.DefaultServerPort;
                }
                this.port = value;
            }
        }

        /// <summary>
        /// Machine function for the server.
        /// </summary>
        [Id(3)]
        public string MachineFunction { get; set; }

        /// <summary>
        /// Datacenter location of the server.
        /// </summary>
        [Id(4)]
        public string Datacenter { get; set; }

        public object Clone()
        {
            return new ServerInfo
                   {
                       Hostname = this.Hostname,
                       Port = this.Port,
                       MachineFunction = this.MachineFunction,
                       Datacenter = this.Datacenter
                   };
        }

        public int CompareTo(ServerInfo other)
        {
            if (other == null)
            {
                return -1;
            }
            var cmp = StringComparer.OrdinalIgnoreCase.Compare(this.Hostname, other.Hostname);
            return cmp != 0 ? cmp : (this.Port == other.Port ? 0 : (this.Port > other.Port ? 1 : -1));
        }

        public override string ToString()
        {
            return this.Hostname + ':' + this.Port;
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as ServerInfo);
        }

        public bool Equals(ServerInfo other)
        {
            return this.CompareTo(other) == 0;
        }

        public override int GetHashCode()
        {
            return this.Hostname.ToLowerInvariant().GetHashCode() + this.Port;
        }
    }
}
