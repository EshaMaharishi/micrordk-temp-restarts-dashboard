package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
    "strconv"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	data "go.viam.com/api/app/data/v1"
	app "go.viam.com/api/app/v1"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/robot/client"
	"go.viam.com/utils/rpc"
//	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const latexHdr string = `#+LATEX: \begin{adjustbox}{width={\textwidth},keepaspectratio}
#+ATTR_LATEX: :placement [!h]  
#+LATEX: \centering
`
const latexFtr string = `#+LATEX: \end{adjustbox}
`

func connectToApp(ctx context.Context, logger logging.Logger, api_key_id string, api_key string) (*rpc.ClientConn, error) {
	conn, err := rpc.DialDirectGRPC(ctx, "app.viam.com:443", logger.Desugar().Sugar(),
		rpc.WithEntityCredentials(api_key_id,
			rpc.Credentials{
				Type:    rpc.CredentialsTypeAPIKey,
				Payload: api_key,
			},
		),
	)
	return &conn, err
}

type Robot struct {
	r       *app.Robot
	orgId   string
	locName string
}

func toRobotStruct(robots []*app.Robot, orgId string, locName string) []Robot {
	var r []Robot
	for _, robot := range robots {
		r = append(r, Robot{
			r:       robot,
			orgId:   orgId,
			locName: locName,
		})
	}
	return r
}

func getAllRobotsForLoc(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, loc_name string) ([]Robot, error) {
	var orgs *app.ListOrganizationsResponse
	app_client := app.NewAppServiceClient(*conn)
	{
		req := app.ListOrganizationsRequest{}

		resp, err := app_client.ListOrganizations(ctx, &req)

		if err != nil {
			return nil, err
		}
		orgs = resp
	}

	for _, org := range orgs.Organizations {
		req := app.ListLocationsRequest{
			OrganizationId: org.Id,
		}
		resp, err := app_client.ListLocations(ctx, &req)
		if err != nil {
			return nil, err
		}
		for _, loc := range resp.Locations {
			if loc_name == loc.Name {
				req_robot := app.ListRobotsRequest{
					LocationId: loc.Id,
				}
				resp_robot, err := app_client.ListRobots(ctx, &req_robot)
				if err != nil {
					return nil, err
				}
				return toRobotStruct(resp_robot.Robots, org.Id, loc_name), nil
			}
		}
	}
	return nil, nil
}

type RobotData struct {
	Name          string
	Location      string
	isLive        bool
	canConnect    bool
	lastDataPoint time.Time
	timestamps    []TimestampData
	pairs         []Pair
}

func connectToRobot(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, r *app.Robot) (bool, error) {
	p, err := getFirstRobotPart(ctx, conn, logger, r)
	if err != nil {
		return false, err
	}
	s, err := getApiKey(ctx, conn, logger, r)
	if err != nil {
		return false, err
	}
	for i := 0; i < 4; i++ {
		c, err := client.New(ctx, p.Fqdn, logger,
			client.WithDialOptions(
				rpc.WithEntityCredentials(s.Id, rpc.Credentials{Type: rpc.CredentialsTypeAPIKey, Payload: s.Key})))
		if err == nil {
			c.Close(ctx)
			return true, nil
		}
	}

	return false, err

}

func getApiKey(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, r *app.Robot) (*app.APIKey, error) {
	app_client := app.NewAppServiceClient(*conn)
	req := app.GetRobotAPIKeysRequest{
		RobotId: r.Id,
	}

	resp, err := app_client.GetRobotAPIKeys(ctx, &req)
	if err != nil {
		return nil, err
	}

	return resp.ApiKeys[0].ApiKey, nil
}

func getFirstRobotPart(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, r *app.Robot) (*app.RobotPart, error) {

	app_client := app.NewAppServiceClient(*conn)
	var parts *app.GetRobotPartsResponse
	{
		req := app.GetRobotPartsRequest{
			RobotId: r.Id,
		}
		resp, err := app_client.GetRobotParts(ctx, &req)
		if err != nil {
			return nil, err
		}
		parts = resp
	}
	return parts.Parts[0], nil

}

func queryLastDataForRobot(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, r *app.Robot) ([]*data.TabularData, error) {
	data_client := data.NewDataServiceClient(*conn)
	start, err := time.Parse(time.RFC1123, "Wed, 22 Aug 2024 09:30:00 EST")
	if err != nil {
		return nil, err
	}
	end := time.Now()
	if err != nil {
		return nil, err
	}
	interval := data.CaptureInterval{
		Start: timestamppb.New(start),
		End:   timestamppb.New(end),
	}
	filter := data.Filter{
		Interval:    &interval,
		RobotId:     r.Id,
		LocationIds: []string{r.Location},
	}
	req := data.DataRequest{
		Filter:    &filter,
		Limit:     5,
		SortOrder: data.Order_ORDER_DESCENDING,
	}
	data_req := data.TabularDataByFilterRequest{
		DataRequest: &req,
		CountOnly:   false,
	}
	resp, err := data_client.TabularDataByFilter(ctx, &data_req)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

func jsonToBson(j string) ([]byte, error) {
	var doc bson.D
	err := bson.UnmarshalExtJSON([]byte(j), true, &doc)
	if err != nil {
		return nil, err
	}
	b, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type TimestampData struct {
	Time      time.Time
	Timestamp uint64
}

// convertBsonToNative recursively converts BSON datetime objects to Go DateTime and BSON arrays to slices of interface{}.
// For slices and maps of specific types, the best we can do is use interface{} as the container type.
func convertBsonToNative(data interface{}) interface{} {
	switch v := data.(type) {
	case primitive.DateTime:
		return v.Time().UTC()
	case primitive.A: // Handle BSON arrays/slices
		nativeArray := make([]interface{}, len(v))
		for i, item := range v {
			nativeArray[i] = convertBsonToNative(item)
		}
		return nativeArray
	case bson.M: // Handle BSON maps
		convertedMap := make(map[string]interface{})
		for key, value := range v {
			convertedMap[key] = convertBsonToNative(value)
		}
		return convertedMap
	case map[string]interface{}: // Handle Go maps
		for key, value := range v {
			v[key] = convertBsonToNative(value)
		}
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		return v
	}
}

func convertTimestampReqArray(array [][]byte, logger logging.Logger) ([]TimestampData, error) {
	var data []TimestampData

	for _, byteSlice := range array {
		// Unmarshal each BSON byte slice into a Go map
		obj := map[string]interface{}{}
		if err := bson.Unmarshal(byteSlice, &obj); err != nil {
			return nil, err
		}
		convertedObj := convertBsonToNative(obj).(map[string]interface{})
   //     fmt.Println(convertedObj)

		timeRaw := convertedObj["time_requested"]
        timeValue := timeRaw.(time.Time)

        timestampFloat64 := convertedObj["timestamp"].(float64)
		timestamp := uint64(timestampFloat64)
		data = append(data, TimestampData{Time: timeValue, Timestamp: timestamp})
    }

	return data, nil
}

func queryTimestampSensorForRobot(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, r Robot) ([]TimestampData, error) {
	data_client := data.NewDataServiceClient(*conn)
	format := "2006-01-02 15:04:05.000 -0700 MST"
	start, err := time.Parse(format, "2024-08-23 18:00:00.000 -0400 EDT")
	if err != nil {
		return nil, err
	}

	end := time.Now()
	if err != nil {
		return nil, err
	}

	var query [][]byte

	component_name := "memory-debug"
	if strings.Contains(r.locName, "deadlock") {
		component_name = "esp32-sensor"
	}
	doc := []bson.D{{
		{"$match", bson.D{
			{"organization_id", fmt.Sprintf("%s", r.orgId)},
			{"component_name", fmt.Sprintf("%s", component_name)},
			{"robot_id", fmt.Sprintf("%s", r.r.Id)},
			{"time_requested", bson.D{
				{"$gte", primitive.NewDateTimeFromTime(start)},
				{"$lte", primitive.NewDateTimeFromTime(end)},
			}},
		}},
	}}

	b, err := bson.Marshal(doc[0])
	if err != nil {
		return nil, err
	}
	query = append(query, b)

	b, err = jsonToBson(`{ "$sort": { "time_received" : 1 } },`)
	if err != nil {
		return nil, err
	}
	query = append(query, b)

	b, err = jsonToBson(` { "$project": { "timestamp": "$data.readings.timestamp",    "time_requested": 1 } },`)
	if err != nil {
		return nil, err
	}
	query = append(query, b)

	td := data.TabularDataByMQLRequest{
		OrganizationId: r.orgId,
		MqlBinary:      query,
	}
	resp, err := data_client.TabularDataByMQL(ctx, &td)
	if err != nil {
		logger.Errorf("error %v", err)
		return nil, err
	}
	data, err := convertTimestampReqArray(resp.RawData, logger)
	if err != nil {
		logger.Errorf("error %v", err)
		return nil, err
	}
	return data, nil
}

func gatherRobotData(ctx context.Context, conn *rpc.ClientConn, logger logging.Logger, r Robot, do_timestamp bool) RobotData {
	data := RobotData{}
	app_client := app.NewAppServiceClient(*conn)
	req := app.GetLocationRequest{
		LocationId: r.r.Location,
	}
	resp, err := app_client.GetLocation(ctx, &req)
	if err != nil {
		data.Location = ""
	} else {
		data.Location = resp.Location.Name
	}

	data.Name = r.r.Name
	tdata, err := queryLastDataForRobot(ctx, conn, logger, r.r)
	if err == nil {
		if len(tdata) == 0 {
			t, _ := time.Parse(time.RFC1123, "Mon, 1 Jan 2024 08:30:00 EST")
			data.lastDataPoint = t
		} else {
			data.lastDataPoint = tdata[0].TimeReceived.AsTime()
		}
	}
	canConnnect, err := connectToRobot(ctx, conn, logger, r.r)
	data.canConnect = canConnnect

	p, err := getFirstRobotPart(ctx, conn, logger, r.r)
	if err != nil {
		data.isLive = false
	} else {
		dt := time.Now().Sub(p.LastAccess.AsTime())
		if dt < time.Minute*30 {
			data.isLive = true
		}
	}
	if do_timestamp {
		timestamps, err := queryTimestampSensorForRobot(ctx, conn, logger, r)
		if err != nil {
			logger.Infof("Err %v", err)
		}
		data.timestamps = timestamps
		pairs := processTimestampData(data, logger)
		data.pairs = pairs
	}
	return data
}

type Pair struct {
	I, J int
}

func DiffDesc[T any](arr []T, f func(T) uint64) []Pair {
	result := []Pair{}

	for i := 0; i < len(arr)-1; i++ {
		if f(arr[i]) > f(arr[i+1]) {
			result = append(result, Pair{I: i, J: i + 1})
		}
	}
	return result
}

func processTimestampData(robot RobotData, logger logging.Logger) []Pair {
	pairs := DiffDesc(robot.timestamps, func(t TimestampData) uint64 {
		return t.Timestamp
	})
	return pairs
}

func converTimeDuration(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	return fmt.Sprintf("%d d %02d:%02d:%02d", days, hours, minutes, seconds)
}

type SubTable struct {
	hdr  table.Row
	hdr2 table.Row
	rows []table.Row
}

func makeSubTable(start int, end int, max int, botdata []RobotData, logger logging.Logger) SubTable {
	var tab SubTable
	var hdr table.Row
	var hdr2 table.Row
	formatIso := "2006-01-02 15:04"
	hdr = append(hdr, "Name")
	hdr2 = append(hdr2, "Name")
	if end > max {
		end = max
	}
	for i := start; i < end; i++ {
		hdr = append(hdr, fmt.Sprintf("#%d", i))
		hdr = append(hdr, fmt.Sprintf("#%d", i))
		hdr2 = append(hdr2, "Date")
		hdr2 = append(hdr2, "Uptime")
	}
	tab.hdr = hdr
	tab.hdr2 = hdr2
	for _, data := range botdata {
		var line table.Row
		line = append(line, fmt.Sprintf("%s (%s)", data.Name, data.Location))
		for i := start; i < end; i++ {
			if i < len(data.pairs) {
				line = append(line, data.timestamps[data.pairs[i].I].Time.Local().Format(formatIso))
				line = append(line, converTimeDuration(time.Microsecond*time.Duration(data.timestamps[data.pairs[i].I].Timestamp)))
                deviceNum, err := strconv.Atoi(data.Name)
                if err != nil {
                    panic(err)
                }
                if data.Location == "test-deadlock" {
                    deviceNum = deviceNum + 20
                }
                locationToUse := data.Location
                if locationToUse == "office temp" {
                    locationToUse = "office-temp"
                }
                fmt.Printf("%v %s %s %s\n", deviceNum, data.Name, locationToUse, data.timestamps[data.pairs[i].I].Time.Local().Format(formatIso))
			}
		}
		tab.rows = append(tab.rows, line)
	}
	return tab
}

func displayRestartDetected(botdata []RobotData, logger logging.Logger) {
	max_r := 4
	max := 0
	for _, r := range botdata {
		if len(r.pairs) > max {
			max = len(r.pairs)
		}
	}
	rowConfigAutoMerge := table.RowConfig{AutoMerge: true}
	number := max / max_r
	logger.Infof("R %d %d", max, number)
	for i := range number + 1 {
		t := makeSubTable(i*number, (i+1)*number, max, botdata, logger)
		render := table.NewWriter()
		render.SetOutputMirror(os.Stdout)
		//render.SetStyle(table.StyleLight)
		render.AppendHeader(t.hdr, rowConfigAutoMerge)
		render.AppendHeader(t.hdr2)
		render.AppendRows(t.rows)
		render.SetPageSize(1)
		//fmt.Printf("** T%d\r\n", i)
		//fmt.Print(latexHdr)
		//render.RenderMarkdown()
		//fmt.Printf(latexFtr)
	}
}

func displayRobotData(botdata []RobotData) {
	sort.Slice(botdata, func(i, j int) bool {
		return botdata[i].lastDataPoint.Before(botdata[j].lastDataPoint)
	})

	render := table.NewWriter()
	render.SetOutputMirror(os.Stdout)
	render.SetStyle(table.StyleLight)

	render.AppendHeader(table.Row{"Name", "Location", "Last Data Point", "Time Since Last Data Point", "Live", "Can Connect"})
	for _, data := range botdata {
		diff := time.Now().Sub(data.lastDataPoint)
		hours := int(diff.Hours())
		minutes := int(diff.Minutes()) % 60
		seconds := int(diff.Seconds()) % 60

		r := table.Row{data.Name, data.Location, data.lastDataPoint.Local().String(), fmt.Sprintf("%02d:%02d:%02d\n", hours, minutes, seconds), fmt.Sprintf("%t", data.isLive), fmt.Sprintf("%t", data.canConnect)}
		render.AppendRow(r)
	}
	//render.Render()
}

func doAll(key_id string, key string, logger logging.Logger, do_timestamp bool) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	conn, error := connectToApp(ctx, logger, key_id, key)
	if error != nil {
		logger.Fatalf("%v", error)
	}
	var robots []Robot

	r, err := getAllRobotsForLoc(ctx, conn, logger, "test-deadlock")
	if err != nil {
		logger.Errorf("couldn't get robots %w", err)
	}
	robots = append(robots, r...)
	r, err = getAllRobotsForLoc(ctx, conn, logger, "office temp")
	if err != nil {
		logger.Errorf("couldn't get robots %w", err)
	}
	robots = append(robots, r...)
	var wg sync.WaitGroup
	robotChan := make(chan RobotData, len(robots))
	for _, robot := range robots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := gatherRobotData(ctx, conn, logger, robot, do_timestamp)
			robotChan <- data
		}()
	}
	wg.Wait()
	close(robotChan)
	var datas []RobotData
	for data := range robotChan {
		datas = append(datas, data)
	}

	displayRobotData(datas)
	if do_timestamp {
		displayRestartDetected(datas, logger)
	}
}

func main() {
    fmt.Print("Running (it can take several minutes)...\n")

	apiKeyIdPtr := flag.String("api_key_id", "", "An org api key id")
	apiKeyPtr := flag.String("api_key", "", "An org api key")
	processTimestamps := flag.Bool("process-timestamp", false, "Enable to process timestamps")
	flag.Parse()
	log := logging.NewLogger("Init")
	f, err := os.OpenFile("logs", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	app := logging.NewWriterAppender(f)
	logger := logging.NewBlankLogger("runner")

	logger.AddAppender(app)
	defer f.Close()

	key_id := *apiKeyIdPtr
	key := *apiKeyPtr
	do_timestamp := *processTimestamps

	doAll(key_id, key, logger, do_timestamp)
}
