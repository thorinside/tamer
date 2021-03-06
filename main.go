package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/fromkeith/gorest"
	_ "github.com/lib/pq"
	"github.com/paulmach/go.geo"
	"github.com/paulmach/go.geo/reducers"
	"gopkg.in/gorp.v1"
)

var dbMap *gorp.DbMap

type Trip struct {
	RouteId      string `json:"route_id"`
	ServiceId    string `json:"service_id"`
	TripId       string `json:"trip_id"`
	TripHeadsign string `json:"trip_headsign"`
	DirectionId  string `json:"direction_id"`
	BlockId      string `json:"block_id"`
	ShapeId      string `json:"shape_id"`
}

type Agency struct {
	AgencyName     string `json:"agency_name"`
	AgencyUrl      string `json:"agency_url"`
	AgencyTimezone string `json:"agency_timezone"`
	AgencyLang     string `json:"agency_lang"`
	AgencyPhone    string `json:"agency_phone"`
}

type Calendar struct {
	ServiceId string `json:"service_id"`
	Monday    string `json:"monday"`
	Tuesday   string `json:"tuesday"`
	Wednesday string `json:"wednesday"`
	Thursday  string `json:"thursday"`
	Friday    string `json:"friday"`
	Saturday  string `json:"saturday"`
	Sunday    string `json:"sunday"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

type CalendarDate struct {
	ServiceId     string `json:"service_id"`
	Date          string `json:"date"`
	ExceptionType string `json:"exception_type"`
}

type Route struct {
	RouteId        string `json:"route_id"`
	RouteShortName string `json:"route_short_name"`
	RouteLongName  string `json:"route_long_name"`
	RouteDesc      string `json:"route_desc"`
	RouteType      string `json:"route_type"`
	RouteUrl       string `json:"route_url"`
}

type Shape struct {
	ShapeId         string  `json:"shape_id"`
	ShapePtLat      float64 `json:"shape_pt_lat"`
	ShapePtLon      float64 `json:"shape_pt_lon"`
	ShapePtSequence int     `json:"shape_pt_sequence"`
}

type ShapePath struct {
	ShapeId string `json:"shape_id"`
	Path    string `json:"path"`
}

type StopTime struct {
	TripId        string `json:"trip_id"`
	ArrivalTime   string `json:"arrival_time"`
	DepartureTime string `json:"departure_time"`
	StopId        string `json:"stop_id"`
	StopSequence  string `json:"stop_sequence"`
	PickupType    string `json:"pickup_type"`
	DropOffType   string `json:"drop_off_type"`
}

type Stop struct {
	StopId       string  `json:"stop_id"`
	StopCode     string  `json:"stop_code"`
	StopName     string  `json:"stop_name"`
	StopDesc     string  `json:"stop_desc"`
	StopLat      float64 `json:"stop_lat"`
	StopLon      float64 `json:"stop_lon"`
	ZoneId       string  `json:"zone_id"`
	StopUrl      string  `json:"stop_url"`
	LocationType string  `json:"location_type"`
}

func main() {

	wipePtr := flag.Bool("wipe", false, "wipe the database and reload it")
	flag.Parse()

	var wipe bool = *wipePtr

	dbMap = initDb(wipe)
	defer dbMap.Db.Close()

	if wipe {
		load("8")
	}

	gorest.RegisterService(new(TransitService))
	gorest.RegisterMarshaller("application/json", gorest.NewJSONMarshaller())
	http.Handle("/", gorest.Handle())
	err := http.ListenAndServe(":8787", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func initDb(wipe bool) *gorp.DbMap {

	hostname := os.Getenv("POSTGRES_PORT_5432_TCP_ADDR")
	connectionString := fmt.Sprintf("host=%s user=nealsanche dbname=tamer sslmode=disable", hostname)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(db)

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}

	dbmap.AddTableWithName(Trip{}, "trip")
	dbmap.AddTableWithName(Agency{}, "agency")
	dbmap.AddTableWithName(Calendar{}, "calendar")
	dbmap.AddTableWithName(CalendarDate{}, "calendarDate")
	dbmap.AddTableWithName(Route{}, "route")
	dbmap.AddTableWithName(Shape{}, "shape")
	dbmap.AddTableWithName(StopTime{}, "stopTime")
	dbmap.AddTableWithName(Stop{}, "stop")

	// create the table. in a production system you'd generally
	// use a migration tool, or create the tables via scripts
	if wipe {
		err = dbmap.DropTablesIfExists()
		checkErr(err, "Drop tables failed")
	}
	err = dbmap.CreateTablesIfNotExists()
	checkErr(err, "Creating tables")

	return dbmap
}

func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}

func downloadDataset(url string, output string) {
	log.Println("Downloading", url, "to", output)

	outfile, err := os.Create(output)
	if err != nil {
		log.Println("Error creating", output, "-", err)
		return
	}
	defer outfile.Close()

	response, err := http.Get(url)
	if err != nil {
		log.Println("Error while downloading", url, "-", err)
		return
	}
	defer response.Body.Close()

	n, err := io.Copy(outfile, response.Body)
	if err != nil {
		log.Println("error wile downloading", url, "-", err)
		return
	}

	log.Println(n, "bytes downloaded.")
}

func load(artifactId string) {
	// Download the most recent dataset
	url := fmt.Sprintf("%s", artifactId)
	downloadDataset(url, "/tmp/schedules.zip")
	// delete any existing rows
	err := dbMap.TruncateTables()
	checkErr(err, "TruncateTables failed")

	dbMap.Exec("drop index stoptime_stopid")
	dbMap.Exec("drop index stoptime_tripid")
	dbMap.Exec("drop index trip_serviceid")
	dbMap.Exec("drop index trip_tripid")
	dbMap.Exec("drop index trip_routeid")

	r, err := zip.OpenReader("/tmp/schedules.zip")
	if err != nil {
		log.Printf("Zip opening failed. Abort load.")
		return
	}
	defer r.Close()

	// Iterate through the files in the archive,
	// printing some of their contents.
	for _, f := range r.File {
		log.Printf("Processing %s...", f.Name)
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
		}

		fileName := path.Base(f.Name)

		transaction, dbMapErr := dbMap.Begin()
		checkErr(dbMapErr, "Starting Transaction")

		reader := csv.NewReader(rc)
		reader.FieldsPerRecord = -1
		rawCSVdata, err := reader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}

		records := 0

		for i := 1; i < len(rawCSVdata); i++ {

			records++

			if records > 15000 {
				checkErr(transaction.Commit(), "Commiting Transaction")
				transaction, dbMapErr = dbMap.Begin()
				checkErr(dbMapErr, "Starting Transaction")
				records = 0
			}

			switch fileName {
			case "trips.txt":
				trip := Trip{
					RouteId:      rawCSVdata[i][0],
					ServiceId:    rawCSVdata[i][1],
					TripId:       rawCSVdata[i][2],
					TripHeadsign: rawCSVdata[i][3],
					DirectionId:  rawCSVdata[i][4],
					BlockId:      rawCSVdata[i][5],
					ShapeId:      rawCSVdata[i][6],
				}
				err := transaction.Insert(&trip)
				checkErr(err, "Inserting record")
			case "agency.txt":
				agency := Agency{
					AgencyName:     rawCSVdata[i][0],
					AgencyUrl:      rawCSVdata[i][1],
					AgencyTimezone: rawCSVdata[i][2],
					AgencyLang:     rawCSVdata[i][3],
					AgencyPhone:    rawCSVdata[i][4],
				}
				err := transaction.Insert(&agency)
				checkErr(err, "Inserting record")
			case "calendar.txt":
				calendar := Calendar{
					ServiceId: rawCSVdata[i][0],
					Monday:    rawCSVdata[i][1],
					Tuesday:   rawCSVdata[i][2],
					Wednesday: rawCSVdata[i][3],
					Thursday:  rawCSVdata[i][4],
					Friday:    rawCSVdata[i][5],
					Saturday:  rawCSVdata[i][6],
					Sunday:    rawCSVdata[i][7],
					StartDate: rawCSVdata[i][8],
					EndDate:   rawCSVdata[i][9],
				}
				err := transaction.Insert(&calendar)
				checkErr(err, "Inserting record")
			case "calendar_dates.txt":
				calendarDate := CalendarDate{
					ServiceId:     rawCSVdata[i][0],
					Date:          rawCSVdata[i][1],
					ExceptionType: rawCSVdata[i][2],
				}
				err := transaction.Insert(&calendarDate)
				checkErr(err, "Inserting record")
			case "routes.txt":
				route := Route{
					RouteId:        rawCSVdata[i][0],
					RouteShortName: rawCSVdata[i][1],
					RouteLongName:  rawCSVdata[i][2],
					RouteDesc:      rawCSVdata[i][3],
					RouteType:      rawCSVdata[i][4],
					RouteUrl:       rawCSVdata[i][5],
				}
				err := transaction.Insert(&route)
				checkErr(err, "Inserting record")
			case "shapes.txt":
				lat, _ := strconv.ParseFloat(strings.TrimSpace(rawCSVdata[i][1]), 64)
				lon, _ := strconv.ParseFloat(strings.TrimSpace(rawCSVdata[i][2]), 64)
				seq, _ := strconv.Atoi(rawCSVdata[i][3])
				shape := Shape{
					ShapeId:         rawCSVdata[i][0],
					ShapePtLat:      lat,
					ShapePtLon:      lon,
					ShapePtSequence: seq,
				}
				err := transaction.Insert(&shape)
				checkErr(err, "Inserting record")
			case "stop_times.txt":
				stopTime := StopTime{
					TripId:        rawCSVdata[i][0],
					ArrivalTime:   rawCSVdata[i][1],
					DepartureTime: rawCSVdata[i][2],
					StopId:        rawCSVdata[i][3],
					StopSequence:  rawCSVdata[i][4],
					PickupType:    rawCSVdata[i][5],
					DropOffType:   rawCSVdata[i][6],
				}
				err := transaction.Insert(&stopTime)
				checkErr(err, "Inserting record")
			case "stops.txt":
				lat, _ := strconv.ParseFloat(strings.TrimSpace(rawCSVdata[i][4]), 64)
				lon, _ := strconv.ParseFloat(strings.TrimSpace(rawCSVdata[i][5]), 64)
				stop := Stop{
					StopId:       rawCSVdata[i][0],
					StopCode:     rawCSVdata[i][1],
					StopName:     rawCSVdata[i][2],
					StopDesc:     rawCSVdata[i][3],
					StopLat:      lat,
					StopLon:      lon,
					ZoneId:       rawCSVdata[i][6],
					StopUrl:      rawCSVdata[i][7],
					LocationType: rawCSVdata[i][8],
				}
				err := transaction.Insert(&stop)
				checkErr(err, "Inserting record")
			}
		}

		rc.Close()
		fmt.Println()

		transaction.Commit()
	}

	log.Println("Generating Indexes")

	dbMap.Exec("create index stoptime_stopid on stoptime (stopid)")
	dbMap.Exec("create index stoptime_tripid on stoptime (tripid)")
	dbMap.Exec("create index trip_serviceid on trip (serviceid)")
	dbMap.Exec("create index trip_tripid on trip (tripid)")
	dbMap.Exec("create index trip_routeid on trip (routeid)")

	err = os.Remove("/tmp/schedules.zip")
	checkErr(err, "deleting /tmp/schedules.zip")

	log.Println("Finished.")
}

type TransitService struct {
	gorest.RestService  `root:"/tamer-v2/" consumes:"application/json" produces:"application/json"`
	agency              gorest.EndPoint `method:"GET" path:"/agency" output:"Agency"`
	findStop            gorest.EndPoint `method:"GET" path:"/findStop/{stopCode:string}" output:"Stop"`
	routes              gorest.EndPoint `method:"GET" path:"/routes/{stopCode:string}" output:"[]Route"`
	calendar            gorest.EndPoint `method:"GET" path:"/calendar" output:"[]Calendar"`
	calendars           gorest.EndPoint `method:"GET" path:"/calendars/{year:string}/{month:string}/{day:string}" output:"[]Calendar"`
	exceptions          gorest.EndPoint `method:"GET" path:"/exceptions/{date:string}" output:"[]CalendarDate"`
	service             gorest.EndPoint `method:"GET" path:"/service" output:"[]string"`
	allCalendars        gorest.EndPoint `method:"GET" path:"/calendars" output:"[]Calendar"`
	findRoute           gorest.EndPoint `method:"GET" path:"/findroute/{shortName:string}" output:"[]Route"`
	stopsForRoute       gorest.EndPoint `method:"GET" path:"/stops/{routeId:string}/{directionId:string}" output:"[]Stop"`
	stopsInRange        gorest.EndPoint `method:"GET" path:"/stops/{lon:string}/{lat:string}/{distance:string}" output:"[]Stop"`
	nearestStopForRoute gorest.EndPoint `method:"GET" path:"/stop/{routeId:string}/{directionId:string}/{lon:string}/{lat:string}" output:"Stop"`
	shape               gorest.EndPoint `method:"GET" path:"/shape/{routeId:string}/{directionId:string}" output:"[]ShapePath"`
	shapeById           gorest.EndPoint `method:"GET" path:"/shape/{shapeId:string}" output:"[]ShapePath"`
	stopSchedule        gorest.EndPoint `method:"GET" path:"/schedule/{stopId:string}/{routeId:string}" output:"[]StopTime"`
	tripSchedule        gorest.EndPoint `method:"GET" path:"/schedule/{tripId:string}" output:"[]StopTime"`
	trip                gorest.EndPoint `method:"GET" path:"/trip/{tripId:string}" output:"[]Trip"`
	trips               gorest.EndPoint `method:"GET" path:"/trips/{routeId:string}" output:"[]Trip"`
	reload              gorest.EndPoint `method:"POST" path:"/admin/data/reload" postdata:"string"`
}

func (serv TransitService) Reload(artifactId string) {
	log.Println(artifactId)
	go load(artifactId)
}

func (serv TransitService) Agency() Agency {

	var agency Agency
	err := dbMap.SelectOne(&agency, "select * from agency")
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return agency
}

func (serv TransitService) TripSchedule(tripId string) []StopTime {
	all := []StopTime{}

	query := "select * from stoptime where tripid = :tripId " +
		" order by arrivaltime"

	_, err := dbMap.Select(&all, query, map[string]interface{}{
		"tripId": tripId,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) Trip(tripId string) []Trip {
	all := []Trip{}

	query := "select * from trip where tripid = :tripId " +
		" order by tripid"

	_, err := dbMap.Select(&all, query, map[string]interface{}{
		"tripId": tripId,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) Trips(routeId string) []Trip {
	all := []Trip{}

	services := serv.currentServiceList()

	query := "select * from trip where serviceid in (" + services + ") and routeid = :routeId"

	_, err := dbMap.Select(&all, query, map[string]interface{}{
		"routeId": routeId,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) StopSchedule(stopId string, routeId string) []StopTime {
	all := []StopTime{}

	services := serv.currentServiceList()

	query := "select * from stoptime where tripid in " +
		"(select tripid from trip where serviceid in (" + services + ") and routeid = :routeId ) " +
		"and stopid = :stopId order by arrivaltime"

	_, err := dbMap.Select(&all, query, map[string]interface{}{
		"routeId": routeId,
		"stopId":  stopId,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) Shape(routeId string, directionId string) []ShapePath {
	all := []ShapePath{}

	services := serv.currentServiceList()

	query := "select * from shape where shapeid in " +
		"(select shapeid from trip where routeid = :route and directionid = :direction and serviceid in (" + services + ")) " +
		"order by shapeid, shapeptsequence"

	shapes := []Shape{}

	_, err := dbMap.Select(&shapes, query, map[string]interface{}{
		"route":     routeId,
		"direction": directionId,
	})
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	var currentShape string
	points := [][2]float64{}

	for _, shape := range shapes {
		if currentShape != shape.ShapeId {

			if len(points) > 0 {
				path := geo.NewPathFromXYData(points)

				reducedPath := reducers.DouglasPeucker(path, 1.0e-5)
				encodedString := reducedPath.Encode()
				all = append(all, ShapePath{
					ShapeId: currentShape,
					Path:    encodedString,
				})
			}

			points = [][2]float64{}
			currentShape = shape.ShapeId
		}

		points = append(points, [2]float64{shape.ShapePtLon, shape.ShapePtLat})

	}

	// Make sure we're not missing the last shape
	if len(points) > 0 {
		path := geo.NewPathFromXYData(points)

		reducedPath := reducers.DouglasPeucker(path, 1.0e-5)
		encodedString := reducedPath.Encode()
		all = append(all, ShapePath{
			ShapeId: currentShape,
			Path:    encodedString,
		})
	}

	return all
}

func (serv TransitService) ShapeById(shapeId string) []ShapePath {
	all := []ShapePath{}

	query := "select * from shape where shapeid = :shapeId order by shapeid, shapeptsequence"

	shapes := []Shape{}

	_, err := dbMap.Select(&shapes, query, map[string]interface{}{
		"shapeId": shapeId,
	})
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	var currentShape string
	points := [][2]float64{}

	for _, shape := range shapes {
		if currentShape != shape.ShapeId {

			if len(points) > 0 {
				path := geo.NewPathFromXYData(points)

				reducedPath := reducers.DouglasPeucker(path, 1.0e-5)
				encodedString := reducedPath.Encode()
				all = append(all, ShapePath{
					ShapeId: currentShape,
					Path:    encodedString,
				})
			}

			points = [][2]float64{}
			currentShape = shape.ShapeId
		}

		points = append(points, [2]float64{shape.ShapePtLon, shape.ShapePtLat})

	}

	// Make sure we're not missing the last shape
	if len(points) > 0 {
		path := geo.NewPathFromXYData(points)

		reducedPath := reducers.DouglasPeucker(path, 1.0e-5)
		encodedString := reducedPath.Encode()
		all = append(all, ShapePath{
			ShapeId: currentShape,
			Path:    encodedString,
		})
	}

	return all
}

func (serv TransitService) NearestStopForRoute(routeId string, directionId string, lon string, lat string) Stop {

	longitude, _ := strconv.ParseFloat(strings.TrimSpace(lon), 64)
	latitude, _ := strconv.ParseFloat(strings.TrimSpace(lat), 64)
	latLongPoint := geo.NewPoint(latitude, longitude)

	stopsForRoute := serv.StopsForRoute(routeId, directionId)

	var nearest Stop
	distance := math.MaxFloat64
	for _, stop := range stopsForRoute {
		stopLatLong := geo.NewPoint(stop.StopLat, stop.StopLon)
		currentDistance := stopLatLong.GeoDistanceFrom(latLongPoint, true)
		if currentDistance < distance {
			nearest = stop
			distance = currentDistance
		}
	}

	return nearest
}

func (serv TransitService) StopsInRange(lon string, lat string, distance string) []Stop {

	longitude, _ := strconv.ParseFloat(strings.TrimSpace(lon), 64)
	latitude, _ := strconv.ParseFloat(strings.TrimSpace(lat), 64)

	rangeToTarget, _ := strconv.ParseFloat(strings.TrimSpace(distance), 64)

	latLongPoint := geo.NewPoint(latitude, longitude)

	all := []Stop{}

	_, err := dbMap.Select(&all, "select * from stop")
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	some := []Stop{}

	for _, stop := range all {
		stopLatLong := geo.NewPoint(stop.StopLat, stop.StopLon)

		if stopLatLong.GeoDistanceFrom(latLongPoint, true) < rangeToTarget {
			some = append(some, stop)
		}
	}

	return some
}

func (serv TransitService) StopsForRoute(routeId string, directionId string) []Stop {

	services := serv.currentServiceList()

	query := "select * from stop where stopid in " +
		"(select distinct stopid from stoptime where tripid in " +
		"(select distinct tripid from trip where routeid = :route and directionid = :direction and serviceid in (" + services + ")" +
		"))"

	all := []Stop{}

	_, err := dbMap.Select(&all, query, map[string]interface{}{
		"route":     routeId,
		"direction": directionId,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) FindStop(stopCode string) Stop {
	var stop Stop
	err := dbMap.SelectOne(&stop, "select * from stop where stopcode = :code", map[string]interface{}{
		"code": stopCode,
	})
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}
	return stop
}

func (serv TransitService) Exceptions(date string) []CalendarDate {
	exceptions := []CalendarDate{}

	_, err := dbMap.Select(&exceptions, "select * from calendardate where date = :date", map[string]interface{}{
		"date": date,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return exceptions
}

func (serv TransitService) FindRoute(shortName string) []Route {
	all := []Route{}

	_, err := dbMap.Select(&all, "select * from route where routeshortname like :name", map[string]interface{}{
		"name": shortName,
	})

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) AllCalendars() []Calendar {
	all := []Calendar{}

	_, err := dbMap.Select(&all, "select * from calendar")

	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return all
}

func (serv TransitService) Calendars(year string, month string, day string) []Calendar {
	date, err := time.Parse("20060102", fmt.Sprintf("%v%v%v", year, month, day))
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return serv.currentService(date)
}

func (serv TransitService) Calendar() []Calendar {
	return serv.currentService(time.Now())
}

func (serv TransitService) Service() []string {
	serviceNames := []string{}

	for _, calendar := range serv.currentService(time.Now()) {
		serviceNames = append(serviceNames, calendar.ServiceId)
	}

	return serviceNames
}

func (serv TransitService) currentService(time time.Time) []Calendar {

	weekdays := []string{"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}

	// Figure out the current service
	weekDay := int(time.Weekday())

	log.Println("Weekday = " + weekdays[weekDay])

	date := time.Format("20060102")

	log.Println("Date = " + date)

	services := []Calendar{}

	query := "select * from calendar where serviceid in " +
		"(select serviceid from calendar " +
		"where startdate <= :date " +
		"and enddate >= :date and " + weekdays[weekDay] + " = '1' " +
		"and serviceid not in " +
		"(select serviceid from calendardate where date = :date and exceptiontype = '2') " +
		") " +
		"or serviceid in " +
		"(select serviceid from calendardate where date = :date and exceptiontype = '1') "

	log.Println(query)

	_, err := dbMap.Select(&services, query,
		map[string]interface{}{
			"date": date,
		})
	checkErr(err, "Query failed")

	return services
}

func (serv TransitService) serviceStringList(services []Calendar) string {

	serviceIds := []string{}
	for _, val := range services {
		serviceIds = append(serviceIds, fmt.Sprintf("'%v'", val.ServiceId))
	}

	serviceString := strings.Join(serviceIds, ",")
	return serviceString
}

func (serv TransitService) currentServiceList() string {
	services := serv.currentService(time.Now())
	return serv.serviceStringList(services)
}

func (serv TransitService) Routes(stopCode string) []Route {

	services := serv.currentServiceList()

	routes := []Route{}

	_, err := dbMap.Select(&routes,
		"select * from route where routeid in "+
			" (select distinct routeid from trip where tripid in "+
			" (select distinct tripid from stoptime where stopid = :stopid) and serviceid in ("+services+"))"+
			" order by routeshortname",
		map[string]interface{}{
			"stopid": stopCode,
		})
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return routes
}
