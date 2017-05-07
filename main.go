package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

var (
	cmd         = `SELECT Year, Month, Day, Hour FROM (Calendar(SELECT "1494114399" AS start, "1494144399" AS end))`
	projectID   string
	credential  string
	jobsService *bigquery.JobsService
)

func init() {
	projectID = "projectID"
	// if projectID = os.Getenv("GOOGLE_PROJECT_ID"); projectID == "" {
	// 	logErr.Fatal("GOOGLE_PROJECT_ID not exist")
	// }

	credential = "credential.json"
	// if credential = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); credential == "" {
	// 	logErr.Fatal("GOOGLE_APPLICATION_CREDENTIALS not exist")
	// }

	// version, err := ioutil.ReadFile("./VERSION")
	// if err != nil {
	// 	logErr.Fatalf("[init] read version err:%v", err)
	// }
}

func main() {
	// create a bigquery client
	jwtConfig, err := NewJWTConfig()
	if err != nil {
		log.Fatalln(err)
	}

	client := jwtConfig.Client(context.Background())
	bigqueryService, err := bigquery.New(client)
	if err != nil {
		log.Fatalln(err)
	}

	// create a jobsService
	jobsService = bigquery.NewJobsService(bigqueryService)

	// create an async job
	jobId, err := jobInsert(cmd)
	if err != nil {
		log.Fatalln(err)
	}

	// polling to check status
	err = jobDone(jobId)
	if err != nil {
		log.Fatalln(err)
	}

	// get result
	jobGetResult(jobId)

}

func jobDone(jobId string) error {
	result, err := jobsService.Get(projectID, jobId).Do()
	if err != nil {
		return err
	}
	for i := 0; i < 100; i++ {
		log.Println(result.Status.State)
		if result.Status.State == "DONE" {
			break
		}
		time.Sleep(time.Second * 1)
		result, err = jobsService.Get(projectID, jobId).Do()
		if err != nil {
			return err
		}
	}
	return nil
}

func jobInsert(cmd string) (string, error) {
	jobConfigQuery := bigquery.JobConfigurationQuery{
		// AllowLargeResults: true,
		// DestinationTable: &bigquery.TableReference{
		// 	DatasetId: "DatasetId",
		// 	ProjectId: projectID,
		// 	TableId:   "TableId",
		// },
		Query: cmd,
		// WriteDisposition:  "WRITE_TRUNCATE",
		// CreateDisposition: "CREATE_IF_NEEDED",
		UserDefinedFunctionResources: []*bigquery.UserDefinedFunctionResource{&bigquery.UserDefinedFunctionResource{
			InlineCode: `
				function Calendar(row, emit){
					var startTime = new Date(row.start*1000), endTime = new Date(row.end*1000);
					while(endTime > startTime){
						emit({YEAR: startTime.getUTCFullYear(), MONTH: startTime.getUTCMonth()+1, Day: startTime.getUTCDate(), Hour: startTime.getUTCHours()});
						startTime.setTime(startTime.getTime()+60*60*1000)
					}
				}

				bigquery.defineFunction(
					'Calendar',                           // Name of the function exported to SQL
					['start', 'end'],                    // Names of input columns
					[{'name': 'YEAR', 'type': 'integer'},  // Output schema
					 {'name': 'MONTH', 'type': 'integer'},
					 {'name': 'Day', 'type': 'integer'},
					 {'name': 'Hour', 'type': 'integer'}
					],
					Calendar                       // Reference to JavaScript UDF
				);`,
		}},
	}
	jobConfig := bigquery.JobConfiguration{Query: &jobConfigQuery}

	result, err := jobsService.Insert(projectID, &bigquery.Job{Configuration: &jobConfig}).Do()
	if err != nil {
		return "", err
	}
	return result.JobReference.JobId, nil
}

func jobGetResult(jobId string) {
	jobsGetQueryResult := jobsService.GetQueryResults(projectID, jobId)
	log.Println("jobsGetQueryResult")
	getQueryResultsResponse, err := jobsGetQueryResult.StartIndex(0).MaxResults(int64(1)).Do()
	if err != nil {
		log.Println("getQueryResultsResponse", err)
	}
	log.Println("getQueryResultsResponse")
	total := getQueryResultsResponse.TotalRows
	log.Println(total)
	speed := uint64(10)
	for startIndex := uint64(0); startIndex < uint64(total/speed+1); startIndex++ {
		getQueryResultsResponse, _ := jobsGetQueryResult.StartIndex(uint64(startIndex * speed)).MaxResults(int64(speed)).Do()
		for _, data := range getQueryResultsResponse.Rows {
			for i, value := range data.F {
				fmt.Printf("%s:%v ", getQueryResultsResponse.Schema.Fields[i].Name, value.V)
			}
			fmt.Printf("\n")
		}
	}
}

// NewJWTConfig returns a new JWT configuration from a JSON key,
// acquired via https://console.developers.google.com.
//
// A config is used to authenticate with Google OAuth2.
func NewJWTConfig() (c *jwt.Config, err error) {
	var keyBytes []byte
	keyBytes, err = ioutil.ReadFile(credential)
	if err != nil {
		return
	}

	c, err = google.JWTConfigFromJSON(
		keyBytes,
		bigquery.BigqueryScope)
	return
}
