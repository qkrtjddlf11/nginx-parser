package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"
)

var (
	totalCount = 0
)

// Kafka Producer configurations
var (
	filePath = flag.String(
		"path",
		"",
		"REQUIRED: File path.\nUsage : -path /var/log/nginx/access.log",
	)
)

type Nginx struct {
	RemoteAddr        string `json:"remote_addr"`
	RemoteUser        string `json:"remote_user"`
	Timestamp         string `json:"@timestamp"`
	RequestMethod     string `json:"method"`
	RequestUrl        string `json:"url"`
	HttpVersion       string `json:"http_version"`
	Status            int    `json:"status"`
	BodyBytesSent     int    `json:"body_bytes_sent"`
	HttpReferer       string `json:"http_referer"`
	HttpUserAgent     string `json:"http_user_agent"`
	HttpXForwardedFor string `json:"http_x_forwarded_for"`
}

func parseNginx(line string) Nginx {
	var buffer bytes.Buffer
	buffer.WriteString(`(\S+)\s`)                    // ) remote_addr
	buffer.WriteString(`(\S+)\s`)                    // ) dash
	buffer.WriteString(`(\S+)\s`)                    // ) remote_user
	buffer.WriteString(`\[([^]]+)\]\s`)              // ) timestamp
	buffer.WriteString(`"(\S*)\s?`)                  // ) request_method
	buffer.WriteString(`(?:((?:[^"]*(?:\\")?)*)\s)`) // ) request_url
	buffer.WriteString(`([^"]*)"\s`)                 // ) http_version
	buffer.WriteString(`(\S+)\s`)                    // 7) status (int)
	buffer.WriteString(`(\S+)\s`)                    // 8) body_bytes_sent (int)
	buffer.WriteString(`"((?:[^"]*(?:\\")?)*)"\s`)   // 8) http_referer
	buffer.WriteString(`"((?:[^"]*(?:\\")?)*)"\s`)   // 8) http_user_agent
	buffer.WriteString(`"(.*)"$`)                    // 8) http_x_forwarded_for

	regex, _ := regexp.Compile(buffer.String())
	matches := regex.FindStringSubmatch(line)
	nginx := Nginx{}

	nginx.RemoteAddr = matches[1]
	nginx.RemoteUser = matches[3]
	nginx.Timestamp = matches[4]
	nginx.RequestMethod = matches[5]
	nginx.RequestUrl = matches[6]
	nginx.HttpVersion = matches[7]
	status, err := strconv.Atoi(matches[8])
	if err != nil {
		status = 0
	}
	nginx.Status = status

	bodyBytesSent, err := strconv.Atoi(matches[9])
	if err != nil {
		bodyBytesSent = 0
	}
	nginx.BodyBytesSent = bodyBytesSent

	nginx.HttpReferer = matches[10]
	nginx.HttpUserAgent = matches[11]
	nginx.HttpXForwardedFor = matches[12]

	return nginx
}

func increaeCount() {
	totalCount++
}

func perfTime(startTime time.Time) func() {
	return func() {
		log.Printf("Elapsed time : %v, Total count : %d\n", time.Since(startTime), totalCount)
	}
}

func main() {
	flag.Parse()

	if *filePath == "" {
		log.Fatal("Need a file path")
	}

	startTime := time.Now()
	defer perfTime(startTime)()

	file, err := os.Open("./access.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	sc := bufio.NewScanner(file)
	for sc.Scan() {
		nginxLog := parseNginx(sc.Text())
		fmt.Println(nginxLog)
		increaeCount()
	}
}
