package main

import "fmt"

const (
	API_VERSIONS_KEY = 18
)

func ProcessRequest(request *Request) Response {
	switch request.Header.ApiKey {
	case API_VERSIONS_KEY:
		return ProcessApiVersionsRequest(request)
	default:
		panic(fmt.Sprintf("API Key unrecognized: %d\n", request.Header.ApiKey))
	}
}

func ProcessApiVersionsRequest(request *Request) *ApiVersionsResponse {
	response := &ApiVersionsResponse{
		Header: &ResponseHeader{
			CorrelationID: request.Header.CorrelationID,
		},
		Body: ApiVersionsResponseBody{
			ErrorCode: 0,
			ApiKeyEntries: []ApiKeyEntry{
				{
					ApiKey:              API_VERSIONS_KEY, // API_VERSIONS
					MinSupportedVersion: 0,
					MaxSupportedVersion: 4,
				},
			},
			ThrottleTimeMS: 0,
		},
	}

	return response
}
