using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;
using SFA.DAS.FundingRuleBridge.Jobs.Activities;
using SFA.DAS.FundingRuleBridge.Jobs.Domain;
using SFA.DAS.FundingRuleBridge.Jobs.Messages;

namespace SFA.DAS.FundingRuleBridge.Jobs.Orchestrators;

public class ProcessJobOrchestrator
{
    [Function(nameof(ProcessJobOrchestrator))]
    public static async Task<bool> RunOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var startTime = context.CurrentUtcDateTime;
        var logger = context.CreateReplaySafeLogger<ProcessJobOrchestrator>();
        var jobInfo = context.GetInput<JobInfo>()!;
        var parameters = new List<KeyValuePair<string, object?>>
        {
            new ("JobId", jobInfo.JobId),
            new ("CorrelationId", context.InstanceId),
        };
        using var scope = logger.BeginScope(parameters);
        try
        {
            var learners = await context.CallActivityAsync<List<LearnerSummary>>(nameof(DownloadAndParseIlrActivity), jobInfo);
            var jobSummary = await RunValidationAsync(context, jobInfo, learners, logger);
            if (jobSummary.JobFailure)
            {
                LogFailure(logger, "Signalled failure by validation engine");
                return false;
            }

            await WriteJobFilesAsync(context, jobInfo, jobSummary, logger);
            LogCompletion(logger, startTime, context.CurrentUtcDateTime, jobSummary);
            return true;
        }
        catch (Exception ex)
        {
            LogFailure(logger, "An exception occurred", ex);
            return false;
        }
    }

    private static void LogCompletion(ILogger logger, DateTime startTime, DateTime endTime, JobSummary jobSummary)
    {
        var duration = endTime - startTime;
        using var _ = logger.BeginScope(new List<KeyValuePair<string, object?>>
        {
            new ("Duration", $"{duration:G}"),
            new ("ValidLearnerCount", jobSummary.ValidLearnerRefs.Count),
            new ("InvalidLearnerCount", jobSummary.InvalidLearnerRefs.Count),
        });
        logger.LogInformation("{OrchestratorName} completed successfully", nameof(ProcessJobOrchestrator));
    }
    
    private static void LogFailure(ILogger logger, string reason, Exception? ex = null)
    {
        using var _ = logger.BeginScope(new List<KeyValuePair<string, object?>>
        {
            new ("Reason", reason)
        });
        logger.LogError(ex, "{OrchestratorName} failed", nameof(ProcessJobOrchestrator));
    }

    private static async Task<JobSummary> RunValidationAsync(TaskOrchestrationContext context, JobInfo jobInfo, List<LearnerSummary> learners, ILogger logger)
    {
        logger.LogInformation("Fan out started");
        var subOrchestrations = learners.Select(learner =>
            context.CallSubOrchestratorAsync<ValidationSummary>(
                nameof(ValidateLearnerOrchestrator),
                new ValidateLearnerMessage
                {
                    JobId = jobInfo.JobId,
                    CorrelationId = context.InstanceId,
                    Ukprn = jobInfo.Ukprn,
                    Uln = learner.LearnRefNumber,
                    DateOfBirth = learner.DateOfBirth,
                    Courses = learner.Courses,
                    Container = jobInfo.Container,
                    Filename = jobInfo.ValidIlrXmlFilename
                }));

        var results = await Task.WhenAll(subOrchestrations);
        logger.LogInformation("Fan in complete");
        return results.ToJobSummary();
    }

    private static async Task WriteJobFilesAsync(TaskOrchestrationContext context, JobInfo jobInfo, JobSummary jobSummary, ILogger logger)
    {
        if (jobSummary.InvalidLearnerRefs is not { Count: > 0 })
        {
            // nothing to write
            logger.LogInformation("Job contained no invalid learners, no files to write");
            return;
        }
        
        logger.LogInformation("Job contained {InvalidLearnerCount} invalid learners", jobSummary.InvalidLearnerRefs.Count);
        var writeSummaryRequest = new WriteJobResultsRequest
        {
            Job = jobInfo,
            ValidationErrors = jobSummary.Items.SelectMany(x => x.ValidationErrors).ToList(),
            InvalidLearnerRefs = jobSummary.InvalidLearnerRefs,
            RuleDescriptions = jobSummary.RuleDescriptions,
        };
        await context.CallActivityAsync(nameof(WriteJobsResultsActivity), writeSummaryRequest);
    }
}