package internal

import (
	"os"
	"regexp"
	"testing"

	tester_utils_testing "github.com/codecrafters-io/tester-utils/testing"
)

func TestStages(t *testing.T) {
	os.Setenv("CODECRAFTERS_RANDOM_SEED", "1234567890")

	testCases := map[string]tester_utils_testing.TesterOutputTestCase{
		"base_stages_pass": {
			UntilStageSlug:      "pv1",
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/base/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"concurrent_stages_pass": {
			StageSlugs:          []string{"nh4", "sk0"},
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/concurrent_stages/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"describe_topic_partitions_pass": {
			// ToDo: Update slugs
			StageSlugs:          []string{"xy1", "xy2", "xy3", "xy4", "xy5", "xy6"},
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/describe_topic_partitions/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"fetch_pass": {
			StageSlugs:          []string{"gs0", "dh6", "hn6", "cm4"},
			CodePath:            "./test_helpers/pass_all",
			ExpectedExitCode:    0,
			StdoutFixturePath:   "./test_helpers/fixtures/fetch/pass",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
	}

	tester_utils_testing.TestTesterOutput(t, testerDefinition, testCases)
}

func normalizeTesterOutput(testerOutput []byte) []byte {
	replacements := map[string][]*regexp.Regexp{
		"hexdump":      {regexp.MustCompile(`[0-9a-fA-F]{4} \| [0-9a-fA-F ]{47} \| .{0,16}`)},
		"session_id":   {regexp.MustCompile(`✔️ .session_id \([0-9]{0,16}\)`)},
		"leader_id":    {regexp.MustCompile(`✔️ .leader_id \([-0-9]{1,}\)`)},
		"leader_epoch": {regexp.MustCompile(`✔️ .leader_epoch \([-0-9]{1,}\)`)},
	}

	for replacement, regexes := range replacements {
		for _, regex := range regexes {
			testerOutput = regex.ReplaceAll(testerOutput, []byte(replacement))
		}
	}

	return testerOutput
}
