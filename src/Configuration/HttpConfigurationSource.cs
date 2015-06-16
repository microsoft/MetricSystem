// ---------------------------------------------------------------------
// <copyright file="HttpConfigurationSource.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration
{
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Cache;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    using Newtonsoft.Json;

    public sealed class HttpConfigurationSource : ConfigurationSource
    {
        private string content = EmptyContent;
        private readonly Timer updateTimer;
        private readonly HttpClient client; 

        public HttpConfigurationSource(string source, TimeSpan updateFrequency)
            : this(new Uri(source), updateFrequency) { }

        public HttpConfigurationSource(Uri source, TimeSpan updateFrequency)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (updateFrequency <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException("updateFrequency");
            }

            this.Source = source;
            this.UpdateFrequency = updateFrequency;
            this.client = new HttpClient(new WebRequestHandler
                                         {
                                             AllowAutoRedirect = true,
                                             AllowPipelining = true,
                                             CachePolicy = new RequestCachePolicy(RequestCacheLevel.CacheIfAvailable),
                                         });
            this.updateTimer = new Timer(_ => this.UpdateContents().Wait(), null, TimeSpan.Zero,
                                         TimeSpan.FromMilliseconds(-1));
        }

        private async Task UpdateContents()
        {
            if (this.Disposed)
            {
                return;
            }

            Events.Write.BeginUpdateHttpConfigurationContent(this.Source);
            var updated = false;
            try
            {
                var response = await this.client.GetAsync(this.Source);
                if (response.IsSuccessStatusCode)
                {
                    var newContent = await response.Content.ReadAsStringAsync();
                    if (!string.Equals(this.content, newContent))
                    {
                        this.content = newContent;
                        this.SetUpdated();
                        updated = true;
                    }
                }
                else
                {
                    Events.Write.UpdateHttpConfigurationRequestFailed(this.Source, response.StatusCode,
                                                                      response.ReasonPhrase);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                if (ex is HttpRequestException || ex is IOException || ex is WebException || ex is ObjectDisposedException)
                {
                    Events.Write.HttpExceptionFromSource(this.Source, ex);
                }
                else
                {
                    throw;
                }
            }

            if (!this.Disposed)
            {
                this.updateTimer.Change(this.UpdateFrequency, TimeSpan.FromMilliseconds(-1));
            }
            Events.Write.EndUpdateHttpConfigurationContent(this.Source, updated);
        }

        public Uri Source { get; private set; }
        public TimeSpan UpdateFrequency { get; private set; }

        protected override JsonReader GetReader()
        {
            return new JsonTextReader(new StringReader(this.content));
        }

        protected override void Dispose(bool disposing)
        {
            this.updateTimer.Dispose();
            this.client.Dispose();
        }
    }
}
