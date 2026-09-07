using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;
using SFA.DAS.FundingRuleBridge.Jobs.Activities;
using SFA.DAS.FundingRuleBridge.Jobs.Domain;
using SFA.DAS.FundingRuleBridge.Jobs.Messages;

namespace SFA.DAS.FundingRuleBridge.Jobs.Orchestrators;

public partial class ValidateLearnerOrchestrator
{
    private const int ValidationTimeoutInMinutes = 10;
    
    [Function(nameof(ValidateLearnerOrchestrator))]
    public static async Task<ValidationSummary> RunOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var logger = context.CreateReplaySafeLogger<ValidateLearnerOrchestrator>();
        var input = context.GetInput<ValidateLearnerMessage>()!;
        var request = new ValidationRequestMessage(
            input.Ukprn,
            input.Uln,
            input.Courses,
            input.CorrelationId,
            context.InstanceId
        );
        var parameters = new List<KeyValuePair<string, object?>>
        {
            new ("CorrelationId", input.CorrelationId),
            new ("WaitingInstanceId", context.InstanceId),
        };
        using var scope = logger.BeginScope(parameters);

        try
        {
            await context.CallActivityAsync(nameof(SendValidationRequestActivity), request);
            LogRequestSent(logger);
        
            var validationResult = await context.WaitForExternalEvent<ValidateLearnerResult>("ValidationComplete", TimeSpan.FromMinutes(ValidationTimeoutInMinutes));
            LogResultReceived(logger);

            return validationResult.ToValidationSummary(request.Uln);
        }
        catch (TaskCanceledException ex)
        {
            logger.LogError(ex, "Timed out waiting for validation result, marking as invalid");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled exception occured, marking as invalid");
        }
        
        // system failure
        return new ValidationSummary(request.Uln, ValidationStatus.SystemError, [], []);
    }

    [LoggerMessage(LogLevel.Debug, "Sent validation request, waiting for result")]
    static partial void LogRequestSent(ILogger logger);

    [LoggerMessage(LogLevel.Debug, "Received validation result")]
    static partial void LogResultReceived(ILogger logger);
}
