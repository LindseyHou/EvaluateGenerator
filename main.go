package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type buildingStatistics struct {
	errorCount        int
	errorDeviceCount  int
	avgErrorPerDevice float32
	alarmCount        int
	alarmDeviceCount  int
}

func main() {
	jsonFile, _ := os.Open("res.json")
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var jsonDatas map[string]map[string]map[string]int
	json.Unmarshal(byteValue, &jsonDatas)
	buildingStatisticsMap := make(map[string]buildingStatistics)
	for projID, value := range jsonDatas {
		buildingStatisticsMap[projID] = buildingStatistics{errorCount: 0, errorDeviceCount: 0, avgErrorPerDevice: 0, alarmCount: 0, alarmDeviceCount: 0}
		if entry, ok := buildingStatisticsMap[projID]; ok {
			for _, v := range value["200"] {
				entry.errorCount += v
				entry.errorDeviceCount += 1
			}
			for _, v := range value["100"] {
				entry.alarmCount += v
				entry.alarmDeviceCount += 1
			}
			if float32(entry.errorDeviceCount) != 0 {
				entry.avgErrorPerDevice = float32(entry.errorCount) / float32(entry.errorDeviceCount)
			}
			buildingStatisticsMap[projID] = entry
		}
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	mysql_uri := os.Getenv("MYSQL_URI")
	// MYSQL
	db, err := sql.Open("mysql", mysql_uri)
	if err != nil {
		log.Fatal(err)
	}
	db.SetConnMaxLifetime(0)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	defer db.Close()
	csvFile, err := os.Create("evaluate.csv")
	if err != nil {
		log.Fatal("failed to open file", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()
	bomUtf8 := []byte{0xEF, 0xBB, 0xBF}
	csvFile.Write(bomUtf8)
	if err := csvWriter.Write([]string{
		"建筑物名称",
		"支队",
		"物联网服务商名称",
		"建筑面积",
		"报警系统点位经验值",
		"平台实际接收点位数量",
		"近7日故障点位数",
		"近7日故障出现次数",
		"故障平均反馈次数",
		"近7日报警点位数",
		"近7日报警出现次数",
	}); err != nil {
		log.Fatal("error writing record to file", err)
	}
	var i = 0
	rows, err := db.Query("SELECT id, name,area FROM iot_building_base_info")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			buildingID          string
			buildingName        string
			buildingArea        float32
			sqlBuildingArea     sql.NullFloat64
			expectedDeviceCount float32
			deviceCount         int
			manageOrgNames      []string
			iotProviderNames    []string
		)
		startTime := time.Now()
		err := rows.Scan(&buildingID, &buildingName, &sqlBuildingArea)
		if err != nil {
			log.Fatal(err)
		}
		buildingArea = float32(sqlBuildingArea.Float64)
		expectedDeviceCount = buildingArea / 40 / 0.7
		err = db.QueryRow(
			"SELECT COUNT(*) "+
				"FROM sensor_cgqdw "+
				"WHERE building_id = ?",
			buildingID,
		).Scan(&deviceCount)
		if err != nil {
			log.Fatal(err)
		}
		rows, err := db.Query(
			"SELECT DISTINCT(manager_org_name) "+
				"FROM b_building_jdjclxm "+
				"WHERE building_id = ?",
			buildingID,
		)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var manage_org_name string
			err := rows.Scan(&manage_org_name)
			if err != nil {
				log.Fatal(err)
			}
			manageOrgNames = append(manageOrgNames, manage_org_name)
		}
		rows, err = db.Query(
			"SELECT DISTINCT(iot_provider_id) "+
				"FROM v_iot_building_relation "+
				"WHERE building_id = ?",
			buildingID,
		)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var (
				iotProviderID   string
				iotProviderName string
			)
			err := rows.Scan(&iotProviderID)
			if err != nil {
				log.Fatal(err)
			}
			err = db.QueryRow(
				"SELECT name "+
					"FROM v_iot_isv_reg "+
					"WHERE id = ?",
				iotProviderID,
			).Scan(&iotProviderName)
			if err != nil {
				log.Fatal(err)
			}
			iotProviderNames = append(iotProviderNames, iotProviderName)
		}
		buildingStatistics := buildingStatisticsMap[buildingID]
		fmt.Println(i, time.Since(startTime), buildingName)
		// log.Println(
		// 	buildingName,
		// 	manageOrgNames,
		// 	iotProviderNames,
		// 	buildingArea,
		// 	expectedDeviceCount,
		// 	deviceCount,
		// 	buildingStatistics.errorDeviceCount,
		// 	buildingStatistics.errorCount,
		// 	buildingStatistics.avgErrorPerDevice,
		// 	buildingStatistics.alarmDeviceCount,
		// 	buildingStatistics.alarmCount,
		// )
		row := []string{}
		row = append(row, buildingName)
		row = append(row, strings.Join(manageOrgNames, ";"))
		row = append(row, strings.Join(iotProviderNames, ";"))
		row = append(row, fmt.Sprintf("%f", buildingArea))
		row = append(row, fmt.Sprintf("%f", expectedDeviceCount))
		row = append(row, fmt.Sprintf("%d", deviceCount))
		row = append(row, fmt.Sprintf("%d", buildingStatistics.errorDeviceCount))
		row = append(row, fmt.Sprintf("%d", buildingStatistics.errorCount))
		row = append(row, fmt.Sprintf("%f", buildingStatistics.avgErrorPerDevice))
		row = append(row, fmt.Sprintf("%d", buildingStatistics.alarmDeviceCount))
		row = append(row, fmt.Sprintf("%d", buildingStatistics.alarmCount))
		if err := csvWriter.Write(row); err != nil {
			log.Fatal("error writing record to file", err)
		}
		i += 1
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
