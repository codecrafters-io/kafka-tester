package serializer_legacy

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder_legacy"
	kafka_interface "github.com/codecrafters-io/kafka-tester/protocol/kafka_interface_legacy"
	"github.com/google/uuid"
)

func GetEncodedBytes(encodableObject kafka_interface.Encodable) []byte {
	encoder := encoder_legacy.Encoder{}
	encoder.Init(make([]byte, 1024))
	encodableObject.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	return encodedBytes
}

func generateDirectories(paths []string) error {
	for _, path := range paths {
		err := generateDirectory(path)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateDirectory(path string) error {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory at %s: %w", path, err)
	}
	return nil
}

//lint:ignore U1000, this is not used in the codebase currently
func base64ToUUID(base64Str string) (string, error) {
	base64Str = strings.Replace(base64Str, "-", "+", 1)
	base64Str += "=="
	decoded, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return "", fmt.Errorf("error decoding base64 string: %w", err)
	}

	if len(decoded) != 16 {
		return "", fmt.Errorf("decoded byte array is not 16 bytes long")
	}

	uuidValue := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		decoded[0:4],
		decoded[4:6],
		decoded[6:8],
		decoded[8:10],
		decoded[10:16])

	return uuidValue, nil
}

func uuidToBase64(uuidStr string) (string, error) {
	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return "", fmt.Errorf("error parsing UUID string: %w", err)
	}

	base64Str := base64.StdEncoding.EncodeToString(parsedUUID[:])
	// Replace '+' with '-' to match the original format
	base64Str = strings.Replace(base64Str, "+", "-", 1)
	// Remove padding
	base64Str = strings.TrimRight(base64Str, "=")
	return base64Str, nil
}
