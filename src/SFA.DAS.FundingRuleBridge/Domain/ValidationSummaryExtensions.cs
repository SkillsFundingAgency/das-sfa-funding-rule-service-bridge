namespace SFA.DAS.FundingRuleBridge.Jobs.Domain;

public static class ValidationSummaryExtensions
{
    public static JobSummary ToJobSummary(this IEnumerable<ValidationSummary> validationSummaries)
    {
        var items = validationSummaries.ToList();
        var passedValidation = items.Where(x => x.Status == ValidationStatus.Passed).ToList();
        var failedValidation = items.Where(x => x.Status == ValidationStatus.Failed).ToList();
        return new JobSummary
        {
            Items = items,
            ValidLearnerRefs = passedValidation.Select(x => x.Uln).Distinct().ToList(),
            InvalidLearnerRefs = failedValidation.Select(x => x.Uln).Distinct().ToList(),
            RuleDescriptions = items.SelectMany(x => x.RuleDescriptions).Distinct().ToList(),
        };
    }
}