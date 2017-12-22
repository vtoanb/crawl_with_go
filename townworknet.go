package main

/*
 * This package use crawl company info from townwork.net
 * step 1. crawl list of job-detail-url and add to redis-set "JOB_LIST"
 * step 2. Get all urls from redis-set "JOB_LIST" to crawl company's info
 *         and store to redis-hash with key is job-detail-url
 * step 3. export to csv file
 */

import (
	"fmt"
	"log"
	"strings"

	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/go-redis/redis"
)

var (
	client = redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
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
}

// Write2Redis write company data to redis
func (info CompanyInfo) Write2Redis() {
	client.HSet(info.URL, "Name", info.Name)
	client.HSet(info.URL, "Address", info.Address)
	client.HSet(info.URL, "Phone", info.Phone)
	client.HSet(info.URL, "Business", info.Business)
}

// ParseCompanyInfo parse company info from job
// Args: string of Job's URL
// Return: CompanyInfo
func ParseCompanyInfo(Job string) {
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
}

// ParseJobList return list of job details
// TODO: also return next URL
func ParseJobList(URL string) {
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
	} else {
		return fmt.Sprintf("%v?page=%v", Base, PageNum)
	}
}

// JobListIndex get job list and push to redis
func JobListIndex() {
	NUM := 2467
	URL := "https://townwork.net/kantou/prc_0054/"
	for i := 0; i <= NUM; i++ {
		go ParseJobList(BuildURL(URL, i))
		time.Sleep(200 * time.Millisecond)
	}
}

func main() {
	JobListIndex()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	JobList := client.SMembers("JOB_LIST")
	for i, Job := range JobList.Val() {
		fmt.Printf("Job: %v, %v\r\n", i, Job)
		go ParseCompanyInfo(Job)
		time.Sleep(10 * time.Millisecond)
	}
}
