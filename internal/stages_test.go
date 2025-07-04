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
		// "base_stages_pass": {
		// 	UntilStageSlug:      "pv1",
		// 	CodePath:            "./test_helpers/pass_all",
		// 	ExpectedExitCode:    0,
		// 	StdoutFixturePath:   "./test_helpers/fixtures/base/pass",
		// 	NormalizeOutputFunc: normalizeTesterOutput,
		// },
		// "concurrent_stages_pass": {
		// 	StageSlugs:          []string{"nh4", "sk0"},
		// 	CodePath:            "./test_helpers/pass_all",
		// 	ExpectedExitCode:    0,
		// 	StdoutFixturePath:   "./test_helpers/fixtures/concurrent_stages/pass",
		// 	NormalizeOutputFunc: normalizeTesterOutput,
		// },
		// "describe_topic_partitions_pass": {
		// 	StageSlugs:          []string{"yk1", "vt6", "ea7", "ku4", "wq2"},
		// 	CodePath:            "./test_helpers/pass_all",
		// 	ExpectedExitCode:    0,
		// 	StdoutFixturePath:   "./test_helpers/fixtures/describe_topic_partitions/pass",
		// 	NormalizeOutputFunc: normalizeTesterOutput,
		// },
		// "fetch_pass": {
		// 	StageSlugs:          []string{"gs0", "dh6", "hn6", "cm4", "eg2", "fd8"},
		// 	CodePath:            "./test_helpers/pass_all",
		// 	ExpectedExitCode:    0,
		// 	StdoutFixturePath:   "./test_helpers/fixtures/fetch/pass",
		// 	NormalizeOutputFunc: normalizeTesterOutput,
		// },
		"apiversions_length_field_too_large": {
			StageSlugs:          []string{"pv1"},
			CodePath:            "./test_helpers/scenarios/apiversions_length_field_too_large",
			ExpectedExitCode:    1,
			StdoutFixturePath:   "./test_helpers/fixtures/scenarios/apiversions_length_field_too_large",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
		"apiversions_length_field_too_small": {
			StageSlugs:          []string{"pv1"},
			CodePath:            "./test_helpers/scenarios/apiversions_length_field_too_small",
			ExpectedExitCode:    1,
			StdoutFixturePath:   "./test_helpers/fixtures/scenarios/apiversions_length_field_too_small",
			NormalizeOutputFunc: normalizeTesterOutput,
		},
	}

	tester_utils_testing.TestTesterOutput(t, testerDefinition, testCases)
}

func normalizeTesterOutput(testerOutput []byte) []byte {
	replacements := map[string][]*regexp.Regexp{
		"hexdump":                {regexp.MustCompile(`(?m)(^\x1b\[33m\[tester::#[a-zA-Z0-9-]{3}\] \x1b\[0m\x1b\[36m[0-9a-f]{4} \| ([a-f0-9][a-f0-9] ){1,16} *\| [[:ascii:]]{1,16}\x1b\[0m\n?)+`)},
		"session_id":             {regexp.MustCompile(`- .session_id \([0-9]{0,16}\)`)},
		"leader_id":              {regexp.MustCompile(`- .leader_id \([-0-9]{1,}\)`)},
		"leader_epoch":           {regexp.MustCompile(`- .leader_epoch \([-0-9]{1,}\)`)},
		"wrote_file":             {regexp.MustCompile(`- Wrote file to: .*`)},
		"topic_id":               {regexp.MustCompile(`- .topic_id \([0-9]{8}-[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{12}\)`)},
		"compact_records_length": {regexp.MustCompile(`- .compact_records_length \([0-9]{2,}\)`)},
		"batch_length":           {regexp.MustCompile(`- .batch_length \([0-9]{2,}\)`)},
		"crc":                    {regexp.MustCompile(`- .crc \([-0-9]{1,}\)`)},
		"length":                 {regexp.MustCompile(`- .length \([0-9]{1,}\)`)},
		"Name":                   {regexp.MustCompile(`✓ Topic Name: [0-9A-Za-z]{3}`)},
		"UUID":                   {regexp.MustCompile(`✓ Topic UUID: [0-9]{8}-[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{12}`)},
		"value_length":           {regexp.MustCompile(`- .value_length \([0-9]{1,}\)`)},
		"value":                  {regexp.MustCompile(`- .[vV]alue \("[A-Za-z0-9 !]{1,}"\)`)},
		"name":                   {regexp.MustCompile(`- .name \([A-Za-z -]{1,}\)`)},
		"topic_name":             {regexp.MustCompile(`- .topic_name \([A-Za-z0-9 ]{1,}\)`)},
		"next_cursor":            {regexp.MustCompile(`- .next_cursor \(\{[A-Za-z0-9 ]{1,}\}\)`)},
		"Messages":               {regexp.MustCompile(`✓ Messages: \["[A-Za-z !]{1,}"\]`)},
		"Topic Name":             {regexp.MustCompile(`✓ TopicResponse\[[0-9]{1,}\] Topic Name: [A-Za-z -]{3,}`)},
		"Topic UUID":             {regexp.MustCompile(`✓ TopicResponse\[[0-9]{1,}\] Topic UUID: [0-9 -]{1,}`)},
		"Record Value":           {regexp.MustCompile(`✓ Record\[[0-9]{1,}\] Value: [A-Za-z0-9 !]{1,}`)},
		"RecordBatch BaseOffset": {regexp.MustCompile(`✓ RecordBatch\[[0-9]{1,}\] BaseOffset: [0-9]{1,}`)},
	}

	for replacement, regexes := range replacements {
		for _, regex := range regexes {
			testerOutput = regex.ReplaceAll(testerOutput, []byte(replacement))
		}
	}

	return testerOutput
}
