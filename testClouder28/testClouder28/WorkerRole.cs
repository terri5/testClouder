using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;
using WifiBox.Worker.Utils;
using Microsoft.ServiceBus.Messaging;

namespace HubWorker28
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        private EventProcessorHost _host;

        public override void Run()
        {
            
            Trace.TraceInformation("HubWorker is running");
            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            WorkerHelper.InitContext();
            _host = WorkerHelper.GetEventProcessorHostByKey(ConstLib.KEY_SYS);
            // 设置最大并发连接数
            ServicePointManager.DefaultConnectionLimit = 12;
            // 有关处理配置更改的信息，
            // 请参见 http://go.microsoft.com/fwlink/?LinkId=166357 上的 MSDN 主题。
            bool result = base.OnStart();
            //Trace.TraceInformation("HubWorker has been started");
            return result;
        }

        public override void OnStop()
        {
            //Trace.TraceInformation("HubWorker is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();
            try
            {
                _host.UnregisterEventProcessorAsync();
            }
            catch (Exception ex)
            {
                Trace.TraceInformation(ex.StackTrace);
            }
            base.OnStop();

            Trace.TraceInformation("HubWorker has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            await _host.RegisterEventProcessorAsync<ContentFileEventProcessor>();
            // TODO: 将以下逻辑替换为你自己的逻辑。
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(1000);
            }
        }
    }
}
