package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/gorp.v1"

	"code.google.com/p/gorest"
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
	ShapeId         string `json:"shape_id"`
	ShapePtLat      string `json:"shape_pt_lat"`
	ShapePtLon      string `json:"shape_pt_lon"`
	ShapePtSequence string `json:"shape_pt_sequence"`
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
	StopId       string `json:"stop_id"`
	StopCode     string `json:"stop_code"`
	StopName     string `json:"stop_name"`
	StopDesc     string `json:"stop_desc"`
	StopLat      string `json:"stop_lat"`
	StopLon      string `json:"stop_lon"`
	ZoneId       string `json:"zone_id"`
	StopUrl      string `json:"stop_url"`
	LocationType string `json:"location_type"`
}

type Message struct {
	Name string `json:"name"`
}

func main() {

	dbMap = initDb()
	defer dbMap.Db.Close()

	// load()

	gorest.RegisterService(new(TransitService))
	gorest.RegisterMarshaller("application/json", gorest.NewJSONMarshaller())
	http.Handle("/", gorest.Handle())
	err := http.ListenAndServe(":8787", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func initDb() *gorp.DbMap {
	db, err := sql.Open("postgres", "user=nealsanche dbname=tamer sslmode=disable")
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
	err = dbmap.CreateTablesIfNotExists()
	checkErr(err, "Create tables failed")

	return dbmap
}

func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}

func load() {

	// delete any existing rows
	err := dbMap.TruncateTables()
	checkErr(err, "TruncateTables failed")

	r, err := zip.OpenReader("CT_GTransit_Schedule-Feb18-Jun18-2015.zip")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	// Iterate through the files in the archive,
	// printing some of their contents.
	for _, f := range r.File {
		log.Printf("Contents of %s:\n", f.Name)
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
		}

		transaction, dbMapErr := dbMap.Begin()
		checkErr(dbMapErr, "Starting Transaction")

		reader := csv.NewReader(rc)
		reader.FieldsPerRecord = -1
		rawCSVdata, err := reader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}

		log.Println(len(rawCSVdata))

		records := 0

		for i := 1; i < len(rawCSVdata); i++ {

			records++

			if records > 100000 {
				checkErr(transaction.Commit(), "Commiting Transaction")
				transaction, dbMapErr = dbMap.Begin()
				checkErr(dbMapErr, "Starting Transaction")
				records = 0
			}

			switch f.Name {
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
				shape := Shape{
					ShapeId:         rawCSVdata[i][0],
					ShapePtLat:      rawCSVdata[i][1],
					ShapePtLon:      rawCSVdata[i][2],
					ShapePtSequence: rawCSVdata[i][3],
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
				stop := Stop{
					StopId:       rawCSVdata[i][0],
					StopCode:     rawCSVdata[i][1],
					StopName:     rawCSVdata[i][2],
					StopDesc:     rawCSVdata[i][3],
					StopLat:      rawCSVdata[i][4],
					StopLon:      rawCSVdata[i][5],
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
}

type TransitService struct {
	gorest.RestService `root:"/tamer-v2/" consumes:"application/json" produces:"application/json"`
	agency             gorest.EndPoint `method:"GET" path:"/agency" output:"Agency"`
	findStop           gorest.EndPoint `method:"GET" path:"/findStop/{stopCode:string}" output:"Stop"`
	routes             gorest.EndPoint `method:"GET" path:"/routes/{stopCode:string}" output:"[]Route"`
}

func (serv TransitService) Agency() Agency {

	var agency Agency
	err := dbMap.SelectOne(&agency, "select * from agency")
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return agency
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
	serviceString := ""
	var first bool = true
	for _, val := range services {
		if first {
			first = false
		} else {
			serviceString = serviceString + ","
		}
		serviceString = serviceString + "'" + val.ServiceId + "'"
	}
	log.Println("Returning " + serviceString)
	return serviceString
}

func (serv TransitService) Routes(stopCode string) []Route {

	services := serv.currentService(time.Now())

	routes := []Route{}

	_, err := dbMap.Select(&routes,
		"select * from route where routeid in "+
			" (select distinct routeid from trip where tripid in "+
			" (select distinct tripid from stoptime where stopid=:stopid) and serviceid in ("+serv.serviceStringList(services)+"))"+
			" order by routeshortname",
		map[string]interface{}{
			"stopid": stopCode,
		})
	if err != nil {
		serv.ResponseBuilder().SetResponseCode(404).WriteAndOveride([]byte(err.Error()))
	}

	return routes
}
