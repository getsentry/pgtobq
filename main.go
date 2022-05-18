package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/lib/pq"
	"google.golang.org/api/option"
)

const Version = "1.4.0"
const CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"

// var() helps define multiple variables at once.
// flag defines command line arguments; flag.String("schema", "public", "postgres schema")
// defines a command line argument/option schema, with "public" as a default value and "postgres schema" as a help message.
var (
	pgConn      = flag.String("uri", "postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable", "postgres connection uri")
	pgSchema    = flag.String("schema", "public", "postgres schema")
	pgTable     = flag.String("table", "", "postgres table name")
	datasetId   = flag.String("dataset", "", "BigQuery dataset")
	projectId   = flag.String("project", "", "BigQuery project id")
	partitions  = flag.Int("partitions", -1, "Number of per-day partitions, -1 to disable")
	versionFlag = flag.Bool("version", false, "Print program version")
	labelKey    = flag.String("label-key", "", "Combined with --label-value, name for label on BigQuery table metadata")
	labelValue  = flag.String("label-value", "", "Combined with --label-key, value for label on BigQuery table metadata")
	exclude     = flag.String("exclude", "", "columns to exclude")
	ignoreTypes = flag.Bool("ignore-unknown-types", false, "Ignore unknown column types")
)

//struct is a typed collection of fields
type Column struct {
	Name       string
	Type       string
	IsNullable string
}

// Given a PG column schema, outputs corresponding BQ schema.
// *Column is a pointer to a Column object, like in C++.
func (c *Column) ToFieldSchema() (*bigquery.FieldSchema, error) {
	var f bigquery.FieldSchema
	f.Name = c.Name
	f.Required = c.IsNullable == "NO"

	switch c.Type {
	case "varchar", "bpchar", "text", "citext", "xml", "cidr", "inet", "uuid", "bit", "varbit", "bytea", "money", "jsonb":
		f.Type = bigquery.StringFieldType
	case "int2", "int4", "int8":
		f.Type = bigquery.IntegerFieldType
	case "float4", "float8", "numeric":
		f.Type = bigquery.FloatFieldType
	case "bool":
		f.Type = bigquery.BooleanFieldType
	case "timestamptz":
		f.Type = bigquery.TimestampFieldType
	case "date":
		f.Type = bigquery.DateFieldType
	case "timestamp":
		f.Type = bigquery.DateTimeFieldType
	case "time":
		f.Type = bigquery.TimeFieldType
	default:
		return nil, errors.New("Unknown column type: " + c.Type)
	}

	return &f, nil
}

// For a given schema and table name, gets PG schema, returns equivalent BQ schema
func schemaFromPostgres(db *sql.DB, schema, table string) bigquery.Schema {
	rows, err := db.Query(`SELECT column_name, udt_name, is_nullable FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		log.Fatal(err)
	}
	// Defers the execution until surrounding code (rest of the function in this case) has run
	defer rows.Close()
	excludes := strings.Split(*exclude, ",")
	var c Column
	var s bigquery.Schema
	for rows.Next() {
		if err := rows.Scan(&c.Name, &c.Type, &c.IsNullable); err != nil {
			log.Fatal(err)
		}
		if !contains(c.Name, excludes) {
			f, err := c.ToFieldSchema()
			if err == nil {
				s = append(s, f)
			} else if !*ignoreTypes {
				panic(err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return s
}

func contains(s string, haystack []string) bool {
	for i := 0; i < len(haystack); i++ {
		if s == haystack[i] {
			return true
		}
	}
	return false
}

// Given a BigQuerySchema, returns a comma-separated string of column names
func columnsFromSchema(schema bigquery.Schema) string {
	// Typed array/list declaration
	cols := make([]string, len(schema))
	for i, field := range schema {
	    // pq.QuoteIdentifier quotes the input parameter, thereby making it case-sensitive in the ultimate SQL query
		cols[i] = pq.QuoteIdentifier(field.Name)
        if field.Type == bigquery.StringFieldType {
            cols[i] = cols[i] + "::text"
        }
	}
	return strings.Join(cols, ",")
}

func getRowsStream(db *sql.DB, schema bigquery.Schema, pgSchema, table string) io.Reader {
	rows, err := db.Query(fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT %s FROM %s.%s) AS t`, columnsFromSchema(schema), pq.QuoteIdentifier(pgSchema), pq.QuoteIdentifier(table)))
	if err != nil {
		log.Fatal(err)
	}
	// Creates a synchronous in-memory pipe in form of io.Reader and io.Writer pair.
	// Everything written to the PipeWriter can be read from the PipeReader.
	reader, writer := io.Pipe()
	// Below is an anonymous function, invoked as a goroutine, allowing it to execute asynchronously
	go func() {
		defer rows.Close()
		defer writer.Close()
		for rows.Next() {
			var b []byte
			rows.Scan(&b)
			writer.Write(b)
			writer.Write([]byte{'\n'})
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}()
	return reader
}

// init() is automatically called once, before main()
func init() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	if *versionFlag {
		fmt.Fprintf(os.Stderr, "%s version: %s (%s on %s/%s; %s)\n", os.Args[0], Version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.Compiler)
		os.Exit(0)
	}

	if (*labelKey != "" && *labelValue == "") || (*labelKey == "" && *labelValue != "") {
		log.Fatal("!! a label key and a label value are both required if either is passed")
	}

	keyfile := os.Getenv(CREDENTIALS)
	if keyfile == "" {
		log.Fatal("!! missing ", CREDENTIALS)
	}
	// option package contains options for setting a Google API client
	opt := option.WithServiceAccountFile(keyfile)
	// get an empty Context
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *projectId, opt)
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("postgres", *pgConn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

    // Returns a bool
	partitioned := *partitions > -1

	schema := schemaFromPostgres(db, *pgSchema, *pgTable)
	table := client.Dataset(*datasetId).Table(*pgTable)

    // _ is a blank identifier, preventing a compilation error due to an unused variable.
    // Metadata() gets BigQuery table metadata.
	if _, err := table.Metadata(ctx); err != nil {
	    // err = True implies BigQuery table does not exist, and this block creates one
		metadata := &bigquery.TableMetadata{Schema: schema}
		if partitioned {
		    // Memory allocation as well as initialization
			metadata.TimePartitioning = &bigquery.TimePartitioning{
				Expiration: time.Duration(*partitions) * 24 * time.Hour,
			}
		}
		if *labelKey != "" && *labelValue != "" {
			labels := make(map[string]string)
			labels[*labelKey] = *labelValue
			metadata.Labels = labels
		}
		if err := table.Create(ctx, metadata); err != nil {
			log.Fatal(err)
		}
	} else if *labelKey != "" && *labelValue != "" {
	    // Update label(s) when BigQuery table exists and label(s) has been provided
		var tm bigquery.TableMetadataToUpdate
		tm.SetLabel(*labelKey, *labelValue)
		_, err := table.Update(ctx, tm, "")
		if err != nil {
			log.Fatal(err)
		}
	}

	if partitioned {
	    // Append partition to BigQuery table name
		table.TableID += time.Now().UTC().Format("$20060102")
	}

	log.Println("TableID", table.TableID)

	f := getRowsStream(db, schema, *pgSchema, *pgTable)
	// Create a ReaderSource–a source for a load operation that gets data from an io.Reader
	rs := bigquery.NewReaderSource(f)
	rs.SourceFormat = bigquery.JSON
	rs.MaxBadRecords = 0
	rs.Schema = schema
	// Create a Loader that is used to load data into a BigQuery table
	loader := table.LoaderFrom(rs)
	loader.CreateDisposition = bigquery.CreateNever
	loader.WriteDisposition = bigquery.WriteTruncate
	if partitioned {
		loader.SchemaUpdateOptions = []string{"ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"}
	}
	// Initiates a copy job, returning a job pointer
	job, err := loader.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("JobID", job.ID())
	for {
		status, err := job.Status(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if status.Statistics.Details != nil {
			details := status.Statistics.Details.(*bigquery.LoadStatistics)
			log.Println("OutputBytes", details.OutputBytes)
			log.Println("OutputRows", details.OutputRows)
		}
		if status.Done() {
			if status.Err() != nil {
				log.Fatal(status.Err())
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

}
