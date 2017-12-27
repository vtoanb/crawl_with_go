package main

/*
 * This package use crawl company info from townwork.net
 * step 1. crawl list of job-detail-url and add to redis-set "JOB_LIST"
 * step 2. Get all urls from redis-set "JOB_LIST" to crawl company's info
 *         and store to redis-hash with key is job-detail-url
 * step 3. export to csv file
 */

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/go-redis/redis"
)

var (
	client = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       3,  // use default DB
	})
)

// CompanyInfo hold crawled info of company
type CompanyInfo struct {
	Name     string
	Address  string
	Phone    string
	Business string
	URL      string
	Cnt      int
}

// Companies Map, esier to write CSV file
var Companies = make(map[string]CompanyInfo)

// Write2Redis write company data to redis
func (info CompanyInfo) Write2Redis() {
	client.HSet(info.URL, "Name", info.Name)
	client.HSet(info.URL, "Address", info.Address)
	client.HSet(info.URL, "Phone", info.Phone)
	client.HSet(info.URL, "Business", info.Business)
}

// ReadFromRedis read company info from redis
func ReadFromRedis(Key string) CompanyInfo {
	info := CompanyInfo{}
	InfoMap := client.HGetAll(Key).Val()
	info.Name = InfoMap["Name"]
	info.Address = InfoMap["Address"]
	info.Business = InfoMap["Business"]
	info.Phone = InfoMap["Phone"]
	info.Cnt = 1
	return info
}

// ParseCompanyInfo parse company info from job
// Args: string of Job's URL
// Return: CompanyInfo
func ParseCompanyInfo(Job string, FinishChan chan bool) {
	URL := fmt.Sprintf("https://townwork.net%v", Job)
	info := CompanyInfo{}
	doc, err := goquery.NewDocument(URL)
	if err != nil {
		log.Fatal(err)
	}

	doc.Find("dl.job-ditail-tbl-inner").Each(func(i int, s *goquery.Selection) {
		if s.Find("dt").Text() == "社名（店舗名）" {
			info.Name = s.Find("dd p").Text()
		}
		if s.Find("dt").Text() == "会社事業内容" {
			info.Business = s.Find("dd p").Text()
		}
		if s.Find("dt").Text() == "会社住所" {
			info.Address = s.Find("dd p").Text()
		}
		if s.Find("dt").Text() == "問い合わせ番号" {
			// info.Phone = strings.TrimSpace(s.Find("p.detail-tel-ttl").Text())
			// info.Phone = strings.TrimSpace(s.Find("span.detail-tel-ico").Text())
			s.Find("span.detail-tel-ico").Each(func(i int, t *goquery.Selection) {
				if len(info.Phone) == 0 {
					info.Phone += t.Text()
				} else {
					info.Phone += ", " + t.Text()
				}
			})
		}
		if len(info.Phone) == 0 {
			info.Phone = strings.TrimSpace(s.Find("p.detail-tel-ttl").Text())
		}
		info.URL = Job
	})
	info.Write2Redis()
	// info.WriteJsonRedis("COMPANY_LIST_3")
	// inform finish
	FinishChan <- true
}

// ParseJobList return list of job details
// TODO: also return next URL
func ParseJobList(URL string, WaitChan chan bool) {
	defer func() {
		WaitChan <- true
	}()

	doc, err := goquery.NewDocument(URL)
	if err != nil {
		log.Fatal(err)
	}

	doc.Find("div.job-cassette-lst-wrap a").Each(func(i int, s *goquery.Selection) {
		href, exist := s.Attr("href")
		if exist && strings.Contains(href, "detail") {
			fmt.Printf("-> Redis: %v\r\n", href)
			client.SAdd("JOB_LIST", href)
		}
	})
}

// BuildURL return list job paginate URL
func BuildURL(Base string, PageNum int) string {
	if PageNum == 0 {
		return Base
	}
	return fmt.Sprintf("%v?page=%v", Base, PageNum)
}

// JobListIndex get job list and push to redis
func JobListIndex(URL string, MaxPages int) {
	WaitChan := make(chan bool, MaxPages)
	for i := 0; i <= MaxPages; i++ {
		url := BuildURL(URL, i)
		fmt.Printf("%v: %v\r\n", i, url)
		go ParseJobList(url, WaitChan)
		time.Sleep(200 * time.Millisecond)
	}
	for j := 0; j < MaxPages; j++ {
		<-WaitChan
	}
}

// WriteCsv write company info to csv
func WriteCsv(FileName string) {
	file, err := os.Create(FileName)
	checkError("cannot create file!", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// detail-url contains detail
	CompanyData, _ := client.Scan(0, "*detail*", 1000000).Val()
	for _, _URL := range CompanyData {
		_info := ReadFromRedis(_URL)

		if company, ok := Companies[_info.Name]; ok {
			company.Cnt++
			Companies[_info.Name] = company
		} else {
			Companies[_info.Name] = _info
		}
	}

	for _, info := range Companies {
		str := []string{info.Name, info.Business, info.Address, info.Phone, strconv.Itoa(info.Cnt)}
		writer.Write(str)
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func WaitFinish(Chan chan bool, Len int) {
	for i := 0; i < Len; i++ {
		<-Chan
	}
}

func main() {
	// rescue when error occur
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	// get all job-detail-url, WaitTime = 200 Millisecond: 5 request / s
	// MaxPage := 2467
	MaxPage := 2 // Config = 2 to reduce testing time
	JobListIndex("https://townwork.net/kantou/prc_0054/", MaxPage)

	// get company's info
	JobList := client.SMembers("JOB_LIST")
	JobListLen := len(JobList.Val())
	fmt.Println("total jobs: ", JobListLen)
	FinishChan := make(chan bool, JobListLen)
	for i, Job := range JobList.Val() {
		fmt.Printf("Job: %v, %v\r\n", i, Job)
		go ParseCompanyInfo(Job, FinishChan)
		time.Sleep(200 * time.Millisecond)
	}
	for {
		var keys []string
		var err error
		keys, cursor, err := client.Scan(cursor, "", 10).Result()
		if err != nil {
			panic(err)
		}
		n += len(keys)
		if cursor == 0 {
			break
		}
	}

	fmt.Println("wait finish....")
	WaitFinish(FinishChan, JobListLen)

	// export csv
	WriteCsv("test.csv")
}
