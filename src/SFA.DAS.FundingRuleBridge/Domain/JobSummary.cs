namespace SFA.DAS.FundingRuleBridge.Jobs.Domain;

public class JobSummary
{
    public bool JobFailure => Items.Any(x => x.Status == ValidationStatus.SystemError);
    public List<ValidationSummary> Items { get; set; } = [];
    public List<string> InvalidLearnerRefs { get; set; } = [];
    public List<string> ValidLearnerRefs { get; set; } = [];
    public List<RuleDescriptionLookup> RuleDescriptions { get; set; } = [];
}