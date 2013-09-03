package main

// $ curl localhost:9999/articles -H "Last-Event-ID: 33895"

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/donovanhide/eventsource"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var port = flag.Int("port", 9999, "port to run server on")
var dbstring = flag.String("db", "user=jl dbname=jl host=/var/run/postgresql sslmode=disable", "connection string for database")
var timeout = flag.Duration("timeout", 10*time.Second, "timeout for a client disconnection")
var interval = flag.Duration("interval", 5*time.Second, "delay between monitor polls")

// article encodes article data we want to stream out as events
type article struct {
	JournalistedId string    `json:"id"`
	Permalink      string    `json:"permalink"`
	Title          string    `json:"title"`
	Content        string    `json:"text"`
	LastScraped    time.Time `json:"lastscraped"`
	Published      time.Time `json:"published"`
	Source         string    `json:"source"`
	Urls           []string  `json:"urls"`
	Journalists    []string  `json:"journalists"`
	// internal channel
	channel string
}

func (art *article) Id() string {
	return art.JournalistedId
}

func (art *article) Event() string {
	return "article"
}

func (art *article) Data() string {
	out, _ := json.Marshal(art)
	return string(out)
}

type articleRepository struct {
	*sql.DB
}

func (repo *articleRepository) streamArticles(channel, lastEventId string, stream chan *article) (last *article) {
	tx, err := repo.Begin()
	if err != nil {
		glog.Error(err)
		return
	}
	defer tx.Rollback()
	glog.V(2).Infoln("Declaring cursor", lastEventId, channel)
	_, err = tx.Exec(`	DECLARE cur NO SCROLL CURSOR FOR
					  	SELECT id
					  	FROM article
						WHERE id > $1
        				AND ($2 = 'articles' OR srcorg=(SELECT id FROM organisation WHERE shortname=$2)) 
        				ORDER BY id ASC
						`, lastEventId, channel)
	if err != nil {
		glog.Fatalln(err)
		return nil
	}
	for {
		glog.V(2).Infoln("Fetching next 100.")
		rows, err := tx.Query(`FETCH FORWARD 100 FROM cur;`)
		if err != nil {
			glog.Error(err)
			return
		}
		defer rows.Close()
		ids := make([]string, 0)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				glog.Error(err)
				return
			}
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			break
		}
		articles, err := tx.Query(fmt.Sprintf(`SELECT a.id,
						MAX(a.permalink),
						MAX(a.title),
						MAX(a.pubdate),
						MAX(a.lastscraped) AS lastscraped,
						COALESCE(MAX(o.prettyname),''),
						COALESCE(string_agg(u.url,' '),''),
						COALESCE(string_agg(j.prettyname,','),''),
						COALESCE(MAX(c.content),''),
						MAX(o.shortname)
						FROM article a 
						INNER JOIN article_content c 
						ON a.id=c.article_id
						LEFT OUTER JOIN article_url u
						ON a.id=u.article_id
						LEFT OUTER JOIN journo_attr attr
						ON a.id = attr.article_id
						INNER JOIN journo j
						ON attr.journo_id=j.id
						INNER JOIN organisation o
						ON a.srcorg=o.id
						WHERE a.id IN (%s)
						GROUP BY a.id
        				ORDER BY a.id ASC`, strings.Join(ids, ",")))
		if err != nil {
			glog.Error(err)
			return
		}
		for articles.Next() {
			var art article
			var urls, journalists string
			if err := articles.Scan(&art.JournalistedId, &art.Permalink, &art.Title, &art.Published, &art.LastScraped, &art.Source, &urls, &journalists, &art.Content, &art.channel); err != nil {
				glog.Error(err)
				return nil
			}
			art.Urls = strings.Split(urls, " ")
			art.Journalists = strings.Split(journalists, ",")
			glog.V(2).Infof("Got Channel: %s Id: %s", art.channel, art.JournalistedId)
			select {
			case <-time.After(*timeout):
				glog.Warning("Timeout on cursor pump")
				return
			case stream <- &art:
				last = &art
			}
		}
	}
	return
}

func (repo *articleRepository) Replay(channel, lastEventId string) (events chan eventsource.Event) {
	stream := make(chan *article)
	events = make(chan eventsource.Event, 500)
	glog.Infof("Replaying Channel: %s from Last-Event-Id: %s", channel, lastEventId)
	go func() {
		for art := range stream {
			events <- art
		}
		close(events)
	}()
	go func() {
		defer close(stream)
		if last := repo.streamArticles(channel, lastEventId, stream); last != nil {
			glog.Infof("Finished Replaying Channel: %s from Last-Event-ID: %s To: %s", channel, lastEventId, last.JournalistedId)
		} else {
			glog.Infof("Nothing to replay")
		}
	}()
	return
}

func (repo *articleRepository) Monitor(srv *eventsource.Server) {
	stream := make(chan *article)
	defer close(stream)
	var lastEventId string
	if err := repo.QueryRow("SELECT MAX(id) FROM article").Scan(&lastEventId); err != nil {
		glog.Fatal(err)
	}
	glog.Infof("Monitoring from Id: %s onwards ", lastEventId)
	go func() {
		for art := range stream {
			glog.Infof("Publishing Id: %s Channel: %s", art.JournalistedId, art.channel)
			srv.Publish([]string{"articles", art.channel}, art)
		}
	}()
	for {
		glog.V(2).Infof("Polling for anything after: %s", lastEventId)
		if last := repo.streamArticles("articles", lastEventId, stream); last != nil {
			lastEventId = last.JournalistedId
		}
		time.Sleep(*interval)
	}

}

func main() {
	flag.Parse()
	db, err := sql.Open("postgres", *dbstring)
	if err != nil {
		glog.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		glog.Fatal(err)
	}
	repo := &articleRepository{db}
	srv := eventsource.NewServer()
	srv.Register("articles", repo)
	channels := make(map[string]struct{})
	var lock sync.Mutex
	router := mux.NewRouter().StrictSlash(false)
	router.HandleFunc("/", srv.Handler("articles"))
	router.HandleFunc("/{channel}/", func(resp http.ResponseWriter, req *http.Request) {
		channel := mux.Vars(req)["channel"]
		lock.Lock()
		if _, ok := channels[channel]; !ok {
			channels[channel] = struct{}{}
			srv.Register(channel, repo)
		}
		lock.Unlock()
		srv.Handler(mux.Vars(req)["channel"])(resp, req)
	})
	http.Handle("/", router)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		glog.Fatal(err)
	}
	defer l.Close()
	glog.Infof("Listening on port %d", *port)
	go repo.Monitor(srv)
	http.Serve(l, nil)
}
