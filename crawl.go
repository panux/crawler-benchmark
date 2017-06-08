package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"

	"github.com/jadr2ddude/consterr"
	"github.com/shirou/gopsutil/cpu"
)

//structure to store count of completed page indexes
var completed struct {
	sync.RWMutex
	comp int64
}

//structure to store CPU usage data
var cputracker struct {
	sync.Mutex
	total   float64
	samples int
}

//indexing worker goroutine
func runner(urlin chan url.URL, pgout chan page) {
	for url := range urlin {
		pg, _, err := crawl(url)
		if err != nil {
			log.Println(err)
		} else {
			pgout <- pg
			completed.Lock()
			completed.comp++
			if completed.comp%1000 == 0 {
				cputracker.Lock()
				log.Printf("Processed %d pages (%d%%)\n", completed.comp, int(cputracker.total/float64(cputracker.samples)))
				cputracker.Unlock()
			}
			completed.Unlock()
		}
	}
}

//goroutine to send URLs from a channel over the network
func senderRelay(urlin chan url.URL, conn io.Writer) {
	defer func() { log.Println(recover()) }()
	g := gob.NewEncoder(conn)
	for url := range urlin {
		err := g.Encode(&url)
		if err != nil {
			panic(err)
		}
	}
}

//goroutine to recieve URLs from the network and transfer them into a channel
func recieverRelay(urlout chan url.URL, conn io.Reader) {
	defer func() { log.Println(recover()) }()
	g := gob.NewDecoder(conn)
	for {
		var recvurl url.URL
		err := g.Decode(&recvurl)
		if err != nil {
			panic(err)
		}
		urlout <- recvurl
	}
}

//goroutine to filter out previously indexed pages
func dedupURL(urlin, urlout chan url.URL) {
	dedup := make(map[url.URL]bool)
	for url := range urlin {
		if !dedup[url] {
			dedup[url] = true
			urlout <- url
		}
	}
}

//dynamically sized link buffer goroutine
func buffer(urlin, urlout chan url.URL, empty chan bool, shutdown chan struct{}, finalcount chan int64) {
	linkBuffer := []string{}
	cnt := int64(0)
	skp := func(lnk *url.URL, err error) url.URL {
		return *lnk
	}
	for {
		if len(linkBuffer) > 0 {
			select {
			case url := <-urlin:
				linkBuffer = append(linkBuffer, url.String())
			case urlout <- skp(url.Parse(linkBuffer[0])):
				linkBuffer = linkBuffer[1:]
				cnt++
			}
		} else {
			select {
			case url := <-urlin:
				linkBuffer = append(linkBuffer, url.String())
			case _, ok := <-shutdown:
				if ok { //Just pause
					<-shutdown
				} else {
					finalcount <- cnt
				}
				return
			}
		}
	}
}

//goroutine to distribute links to nodes based on hash value
func hashDist(urlin chan url.URL, out []chan url.URL) {
	for url := range urlin {
		hash := crc32.ChecksumIEEE([]byte(url.String()))
		out[hash%uint32(len(out))] <- url
	}
}

//goroutine to filter links from extracted page data
func pageRecurser(pagein chan page, urlout chan url.URL) {
	for pg := range pagein {
		for _, v := range pg.links {
			url := v.address
			if url.Host == "en.wikipedia.org" {
				if url.Scheme == "" {
					url.Scheme = "https"
				}
				urlout <- url
			}
		}
	}
}

//structures used for shutdown and data recording
var shutdownLock sync.RWMutex
var result struct {
	sync.Mutex
	dat []string
}

func main() {
	//Extend TLS timeout due to large number of connections
	http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout = time.Minute

	log.Println("Loading settings. . .")
	var flagInitial string
	var flagNodes string
	var flagConc uint
	flag.StringVar(&flagInitial, "initial", "", "Initial URL")
	flag.StringVar(&flagNodes, "nodelist", "nodes.list", "Node list file")
	flag.UintVar(&flagConc, "conc", 100, "Number of indexing workers to use per CPU thread")
	flag.Parse()
	isMaster := flagInitial != ""

	//Parse initial URL
	var lnk *url.URL
	var err error
	if isMaster {
		lnk, err = url.Parse(flagInitial)
		if err != nil {
			panic(err)
		}
	}

	//Load list of nodes
	d, err := ioutil.ReadFile(flagNodes)
	if err != nil {
		panic(err)
	}
	nodes := strings.Split(string(d), "\n")
	log.Println(nodes)
	log.Println("Finished loading settings")

	log.Println("Preparing for startup. . . ")
	//Pipeline channels
	found := make(chan url.URL, 400)
	dedupsenders := make([]chan url.URL, len(nodes))
	dedup := make(chan url.URL, 50)
	bufin := make(chan url.URL)
	bufout := make(chan url.URL)
	runsenders := bufout
	toindex := make(chan url.URL, 50)
	torecurse := make(chan page, 5)

	empty := make(chan bool)
	shutdown := make(chan struct{})
	finalcount := make(chan int64)

	stop := make(chan struct{})

	//Start stages of Pipeline
	log.Println("Starting local pipeline goroutines. . .")
	numworkers := runtime.NumCPU() * int(flagConc)
	log.Printf("Starting %d indexing workers. . .\n", numworkers)
	for i := 0; i < numworkers; i++ {
		go runner(toindex, torecurse)
	}
	log.Println("Done starting indexing workers")
	go pageRecurser(torecurse, found)
	go dedupURL(dedup, bufin)
	go buffer(bufin, bufout, empty, shutdown, finalcount)
	log.Println("Done starting local pipeline goroutines")

	log.Println("Opening TCP ports 9000-9002")
	dedupnet, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	runnet, err := net.Listen("tcp", ":9001")
	if err != nil {
		panic(err)
	}
	stopnet, err := net.Listen("tcp", ":9002")
	if err != nil {
		panic(err)
	}
	log.Println("Successfully opened TCP ports")

	deduplck := new(sync.Mutex)
	runlck := new(sync.Mutex)
	deduplck.Lock()
	runlck.Lock()
	go func() {
		numconn := 0
		for numconn < len(nodes) {
			conn, err := dedupnet.Accept()
			if err != nil {
				panic(err)
			}
			log.Printf("Deduplication connection from %s\n", conn.RemoteAddr().String())
			go recieverRelay(dedup, conn)
			numconn++
		}
		deduplck.Unlock()
		log.Println("All nodes connected for deduplication")
	}()
	go func() {
		numconn := 0
		for numconn < len(nodes) {
			conn, err := runnet.Accept()
			if err != nil {
				panic(err)
			}
			log.Printf("Runner connection from %s\n", conn.RemoteAddr().String())
			go recieverRelay(toindex, conn)
			numconn++
		}
		runlck.Unlock()
		log.Println("All nodes connected for link processing")
	}()
	if isMaster { //Deal with shutdown management
		go func() {
			for {
				conn, err := stopnet.Accept()
				if err != nil {
					panic(err)
				}
				go func() {
					shutdownLock.RLock()
					defer shutdownLock.RUnlock()
					_, _ = <-stop
					conn.Write([]byte("Completed\nCompleted\n"))
					result.Lock()
					defer result.Unlock()
					bs := bufio.NewScanner(conn)
					bs.Scan()
					result.dat = append(result.dat, bs.Text())
				}()
			}
		}()
	}

	log.Println("Waiting for other nodes to start")
	time.Sleep(time.Minute) //Wait for everything else to start

	log.Println("Connecting to nodes")
	for i := range dedupsenders { //Connect all of the deduplication senders
		conn, err := net.Dial("tcp", nodes[i]+":9000")
		if err != nil {
			panic(err)
		}
		ch := make(chan url.URL, 2)
		go senderRelay(ch, conn)
		dedupsenders[i] = ch
	}
	for _, v := range nodes { //Connect all of the deduplication senders
		conn, err := net.Dial("tcp", v+":9001")
		if err != nil {
			panic(err)
		}
		go senderRelay(runsenders, conn)
	}
	go hashDist(found, dedupsenders)
	deduplck.Lock()
	runlck.Lock()

	log.Println("Starting")
	if isMaster { //Start it up finally!
		found <- *lnk
	}
	go func() { //Monitor CPU in background
		for {
			usage, err := cpu.Percent(time.Second, false)
			if err != nil {
				panic(err)
			}
			cputracker.Lock()
			cputracker.total += usage[0]
			cputracker.samples++
			cputracker.Unlock()
		}
	}()
	log.Println("Started!")

	//Wait for completion and save values when done
	if isMaster {
		time.Sleep(time.Minute * 2)
		completed.Lock()
		result.dat = []string{}
		close(stop)
		shutdownLock.Lock()
		result.dat = append(result.dat, fmt.Sprintf("%d | %f", completed.comp, cputracker.total/float64(cputracker.samples)))
		b, e := json.Marshal(&result.dat)
		if e != nil {
			panic(e)
		}
		e = ioutil.WriteFile("results.json", b, 600)
		if e != nil {
			panic(e)
		}
	} else {
		conn, err := net.Dial("tcp", nodes[0]+":9002")
		if err != nil {
			panic(err)
		}
		bufio.NewScanner(conn).Scan()
		completed.Lock()
		cputracker.Lock()
		conn.Write([]byte(fmt.Sprintf("%d | %f", completed.comp, cputracker.total/float64(cputracker.samples))))
	}
}

//structure for extracted page data
type page struct {
	title      string
	words      map[string]uint
	headings   []heading
	paragraphs []string
	links      []link
}

//structure for extracted heading data
type heading struct {
	text  string
	level int
}

//structure for extracted link data
type link struct {
	address url.URL
	text    string
}

//tool for merging text tokens
type paragraphBuilder string

func newParagraphBuilder() *paragraphBuilder {
	var str = ""
	return (*paragraphBuilder)(&str)
}

func (p *paragraphBuilder) concat(str string) {
	*p += paragraphBuilder(str)
}

func (p *paragraphBuilder) get() string {
	return strings.Replace(string(*p), "\n", "", -1)
}

func (p *paragraphBuilder) reset() {
	*p = ""
}

func (p *paragraphBuilder) next() string {
	defer p.reset()
	return p.get()
}

//function to index a page
func crawl(address url.URL) (pg page, dat []byte, e error) {
	req, err := http.Get(address.String())
	defer func() { e, _ = recover().(error) }()
	buf := bytes.NewBuffer(nil)
	if err != nil {
		panic(err)
	}
	defer req.Body.Close()
	typ, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		panic(err)
	}
	if typ != "text/html" {
		panic(consterr.Error(fmt.Sprintf("Invalid mime type: %s", req.Header.Get("Content-Type"))))
	}
	zz, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
	if err != nil {
		panic(err)
	}
	tokenizer := html.NewTokenizer(req.Body) //Save output while tokenizing
	pg.words = make(map[string]uint)
	pg.headings = []heading{}
	pg.paragraphs = []string{}
	pg.links = []link{}
	ht := make(map[atom.Atom]int) //Header type lookup table
	ht[atom.H1] = 1
	ht[atom.H2] = 2
	ht[atom.H3] = 3
	ht[atom.H4] = 4
	ht[atom.H5] = 5
	ht[atom.H6] = 6
	head := func(n int) {
		txt := ""
		for {
			switch tokenizer.Next() {
			case html.TextToken:
				txt += string(tokenizer.Text())
			case html.EndTagToken:
				if ht[tokenizer.Token().DataAtom] == n {
					pg.headings = append(pg.headings, heading{text: strings.TrimSpace(txt), level: n})
					return
				}
			case html.ErrorToken:
				panic("Error token")
			}
		}
	}
	func() {
		para := newParagraphBuilder()
		isPara := false
		for {
			switch tokenizer.Next() {
			case html.ErrorToken: //We are done
				return
			case html.TextToken:
				if isPara {
					para.concat(string(tokenizer.Text()))
				}
			case html.StartTagToken:
				token := tokenizer.Token()
				switch token.DataAtom {
				case atom.A: //Process link
					addr := func() string {
						for _, v := range token.Attr {
							if v.Key == atom.Href.String() {
								return v.Val
							}
						}
						return ""
					}()
					if addr != "" {
						txt := func() string {
							lnktxt := ""
							for {
								switch tokenizer.Next() {
								case html.EndTagToken:
									tkn := tokenizer.Token()
									if tkn.DataAtom == atom.A {
										return lnktxt
									}
								case html.TextToken:
									lnktxt += string(tokenizer.Text())
									if isPara {
										para.concat(string(tokenizer.Text()))
									}
								case html.ErrorToken:
									panic("Error token")
								}
							}
						}()
						lnk, err := url.Parse(addr)
						if err != nil {
							panic(err)
						}
						if lnk.Host == "" {
							lnk.Host = address.Host
						}
						pg.links = append(pg.links, link{address: *lnk, text: txt})
					}
				case atom.H1:
					head(1)
				case atom.H2:
					head(2)
				case atom.H3:
					head(3)
				case atom.H4:
					head(4)
				case atom.H5:
					head(5)
				case atom.H6:
					head(6)
				case atom.Title:
					pg.title = func() string {
						title := ""
						for {
							switch tokenizer.Next() {
							case html.TextToken:
								title += string(tokenizer.Text())
							case html.EndTagToken:
								return title
							case html.ErrorToken:
								panic("Error token")
							}
						}
					}()
				case atom.P:
					isPara = true
				}
			case html.EndTagToken:
				tkn := tokenizer.Token()
				switch tkn.DataAtom {
				case atom.P:
					isPara = false
					pg.paragraphs = append(pg.paragraphs, para.next())
				}
			case html.SelfClosingTagToken:
				//Ignore
			case html.CommentToken:
				//Ignore
			case html.DoctypeToken:
				//Ignore
			}
		}
	}()
	for _, par := range pg.paragraphs {
		for _, word := range strings.Split(par, " ") {
			pg.words[word]++
		}
	}
	zz.Flush()
	zz.Close()
	dat = buf.Bytes()
	return
}
