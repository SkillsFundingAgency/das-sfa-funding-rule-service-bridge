using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using ESFA.DC.JobContext.Interface;
using ESFA.DC.JobContextManager.Model;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using SFA.DAS.FundingRuleBridge.Jobs.Domain;
using SFA.DAS.FundingRuleBridge.Jobs.Orchestrators;

namespace SFA.DAS.FundingRuleBridge.Jobs.Handlers;

[SuppressMessage("Performance", "CA1873:Avoid potentially expensive logging")]
public class JobContextMessageHandler(ILogger<JobContextMessageHandler> logger): IJobContextMessageHandler
{
    public DurableTaskClient DurableClient { get; set; }
    
    public async Task<bool> HandleAsync(JobContextMessage message, CancellationToken cancellationToken)
    {
        using var handlerScope = logger.BeginScope(new List<KeyValuePair<string, object?>> { new ("JobId", message.JobId) });
        logger.LogInformation("Received JobContextMessage");

        var count = 1;
        var instanceId = $"as-val-{message.JobId}-{count}";
        var existingInstance = await DurableClient.GetInstanceAsync(instanceId, cancellationToken);

        while (existingInstance is not null)
        {
            logger.LogInformation("Found existing instance '{InstanceId}' ({RuntimeStatus}), incrementing instance id", instanceId, existingInstance.RuntimeStatus);
            instanceId = $"as-val-{message.JobId}-{++count}";
            existingInstance = await DurableClient.GetInstanceAsync(instanceId, cancellationToken);
        }
        
        using var instanceScope = logger.BeginScope(new List<KeyValuePair<string, object?>> { new("InstanceId", instanceId) });

        if (!TryGetJobInfo(message, logger, out var jobInfo))
        {
            logger.LogError("Failed to get job info from message");
            return false;
        }
            
        await DurableClient.ScheduleNewOrchestrationInstanceAsync(nameof(ProcessJobOrchestrator), jobInfo, new StartOrchestrationOptions(instanceId), cancellationToken);
        logger.LogInformation("Started AS validation orchestration");

        try
        {
            while (existingInstance is null)
            {
                existingInstance = await WaitForInstance(instanceId);
            }
        }
        catch (Exception e)
        {
            logger.LogError(e, "Unhandled exception whilst waiting for job to complete");
            return false;
        }

        if (existingInstance.RuntimeStatus != OrchestrationRuntimeStatus.Completed)
        {
            logger.LogError("Job did not complete successfully, status: {FinalStatus}", existingInstance.RuntimeStatus);
            return false;
        }

        if (TryGetJobResult(existingInstance, out var jobResult))
        {
            logger.LogInformation("Job completed with result: {JobResult}", jobResult.Value ? "Success" : "Failure");
            return jobResult.Value;
        }

        logger.LogError("Job completed successfully but did not contain a JobResult");
        return false;
    }

    private async Task<OrchestrationMetadata?> WaitForInstance(string instanceId)
    {
        using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        try
        {
            return await DurableClient.WaitForInstanceCompletionAsync(instanceId, true, cts.Token);
        }
        catch (Exception) when (cts.Token.IsCancellationRequested)
        {
            return null;
        }
    }

    private static bool TryGetJobResult(OrchestrationMetadata existingInstance, [NotNullWhen(true)]out bool? jobResult)
    {
        jobResult = null;
        if (existingInstance.SerializedOutput is null)
        {
            return false;
        }
        
        try
        {
            jobResult = JsonSerializer.Deserialize<bool?>(existingInstance.SerializedOutput);
            return jobResult.HasValue;
        }
        catch
        {
            return false;
        }
    }
    
    private static bool TryGetJobInfo(JobContextMessage jobContextMessage, ILogger logger, [NotNullWhen(true)] out JobInfo? jobInfo)
    {
        jobInfo = null;

        if (!jobContextMessage.KeyValuePairs.TryGetValue(JobContextMessageKey.Container, out var container))
        {
            logger.LogError("JobContextMessage does not contain the Container value");
            return false;
        }
        
        if (!jobContextMessage.KeyValuePairs.TryGetValue(JobContextMessageKey.UkPrn, out var ukprn))
        {
            logger.LogError("JobContextMessage does not contain the Ukprn value");
            return false;
        }
        
        if (!jobContextMessage.KeyValuePairs.TryGetValue(JobContextMessageKey.Filename, out var filename))
        {
            logger.LogError("JobContextMessage does not contain the Filename value");
            return false;
        }
        
        jobInfo = new JobInfo
        {
            JobId = jobContextMessage.JobId,
            Ukprn = (string)ukprn,
            Container = (string)container,
            ValidIlrXmlFilename = (string)filename,
        };
        
        return true;
    }
}