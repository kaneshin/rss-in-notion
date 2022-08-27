package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jomei/notionapi"
	"github.com/mmcdole/gofeed"
	"gopkg.in/yaml.v3"
)

type Feed struct {
	URL     string   `yaml:"url"`
	Title   string   `yaml:"title"`
	Tags    []string `yaml:"tags"`
	Expires int      `yaml:"expires"`
}

type Config struct {
	Expires int `yaml:"expires"`
	Clean   struct {
		Status []string `yaml:"status"`
	} `yaml:"clean"`
	Feeds []Feed `yaml:"feeds"`
}

var configPath = flag.String("config", "./config.yml", "")
var notionDatabaseID = notionapi.DatabaseID(os.Getenv("NOTION_DATABASE_ID"))
var client = notionapi.NewClient(notionapi.Token(os.Getenv("NOTION_TOKEN")))

func PropertiesFromItem(feed Feed, item *gofeed.Item) map[string]notionapi.Property {
	props := map[string]notionapi.Property{
		"Name": notionapi.TitleProperty{
			Title: []notionapi.RichText{
				{
					Text: notionapi.Text{
						Content: fmt.Sprintf("%s | %s", item.Title, feed.Title),
					},
				},
			},
		},
		"URL": notionapi.URLProperty{
			URL: item.Link,
		},
	}
	tags := []notionapi.Option{
		{
			Name: feed.Title,
		},
	}
	for _, v := range feed.Tags {
		tag := notionapi.Option{
			Name: v,
		}
		tags = append(tags, tag)
	}
	props["Tags"] = notionapi.MultiSelectProperty{
		MultiSelect: tags,
	}
	if item.PublishedParsed != nil {
		props["Publish"] = notionapi.DateProperty{
			Date: &notionapi.DateObject{
				Start: (*notionapi.Date)(item.PublishedParsed),
			},
		}
	}
	return props
}

func CreatePage(ctx context.Context, feed Feed, item *gofeed.Item) error {
	req := &notionapi.PageCreateRequest{
		Parent: notionapi.Parent{
			Type:       notionapi.ParentTypeDatabaseID,
			DatabaseID: notionDatabaseID,
		},
		Properties: PropertiesFromItem(feed, item),
	}
	_, err := client.Page.Create(ctx, req)
	return err
}

func UpdatePage(ctx context.Context, pageID string, feed Feed, item *gofeed.Item) error {
	req := &notionapi.PageUpdateRequest{
		Properties: PropertiesFromItem(feed, item),
	}
	_, err := client.Page.Update(ctx, notionapi.PageID(pageID), req)
	return err
}

func DeletePage(ctx context.Context, pageID string) error {
	req := &notionapi.PageUpdateRequest{
		Properties: map[string]notionapi.Property{},
		Archived:   true,
	}
	_, err := client.Page.Update(ctx, notionapi.PageID(pageID), req)
	return err
}

func runPull(ctx context.Context, feed Feed) error {
	fp := gofeed.NewParser()
	fd, err := fp.ParseURLWithContext(feed.URL, ctx)
	if err != nil {
		return err
	}
	if feed.Title == "" {
		feed.Title = fd.Title
	}

	// find pages already feeded then collect feed urls.
	req := &notionapi.SearchRequest{
		Query: feed.Title,
		Filter: map[string]interface{}{
			"property": "object",
			"value":    "page",
		},
		Sort: &notionapi.SortObject{
			Timestamp: notionapi.TimestampLastEdited,
			Direction: notionapi.SortOrderDESC,
		},
	}
	res, err := client.Search.Do(ctx, req)
	if err != nil {
		return err
	}
	links := map[string]string{}
	for _, r := range res.Results {
		if p, ok := r.(*notionapi.Page); ok {
			prop, ok := p.Properties["URL"].(*notionapi.URLProperty)
			if !ok {
				continue
			}
			if prop.URL == "" {
				continue
			}
			// set notion page id with url as key.
			links[prop.URL] = p.ID.String()
		}
	}

	// create or update feed items.
	expires := time.Now().Add(-time.Duration(feed.Expires) * time.Second)
	for _, item := range fd.Items {
		if item.PublishedParsed == nil {
			continue
		}
		if item.PublishedParsed.Before(expires) {
			continue
		}
		var err error
		if id, ok := links[item.Link]; ok {
			err = UpdatePage(ctx, id, feed, item)
			if err != nil {
				log.Println(err)
			} else {
				log.Printf("updated %s\n", item.Link)
			}
		} else {
			err = CreatePage(ctx, feed, item)
			if err != nil {
				log.Println(err)
			} else {
				log.Printf("created %s\n", item.Link)
			}
		}
	}
	return nil
}

func runCleanByStatus(ctx context.Context, feed Feed, status []string) error {
	fp := gofeed.NewParser()
	fd, err := fp.ParseURLWithContext(feed.URL, ctx)
	if err != nil {
		return err
	}
	if feed.Title == "" {
		feed.Title = fd.Title
	}

	// find pages already feeded then collect feed urls.
	req := &notionapi.SearchRequest{
		Query: feed.Title,
		Filter: map[string]interface{}{
			"property": "object",
			"value":    "page",
		},
		Sort: &notionapi.SortObject{
			Timestamp: notionapi.TimestampLastEdited,
			Direction: notionapi.SortOrderDESC,
		},
	}
	res, err := client.Search.Do(ctx, req)
	if err != nil {
		return err
	}

	statusMap := map[string]struct{}{}
	for _, v := range status {
		statusMap[v] = struct{}{}
	}
	expires := time.Now().Add(-time.Duration(feed.Expires) * time.Second)
	for _, r := range res.Results {
		if p, ok := r.(*notionapi.Page); ok {
			statusProp, ok := p.Properties["Status"].(*notionapi.SelectProperty)
			if !ok {
				continue
			}
			if _, ok := statusMap[statusProp.Select.Name]; !ok {
				continue
			}
			publishProp, ok := p.Properties["Publish"].(*notionapi.DateProperty)
			if !ok {
				continue
			}
			if publishProp.Date.Start == nil {
				continue
			}
			if (*time.Time)(publishProp.Date.Start).Before(expires) {
				id := p.ID.String()
				err = DeletePage(ctx, id)
				if err != nil {
					log.Println(err)
				} else {
					log.Printf("deleted %s\n", id)
				}
			}
		}
	}
	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	f, err := os.Open(os.ExpandEnv(*configPath))
	if err != nil {
		log.Fatal(err)
	}
	var c Config
	err = yaml.NewDecoder(f).Decode(&c)
	if err != nil {
		log.Fatal(err)
	}

	for k, v := range c.Feeds {
		// set default value to feed if needed
		if v.Expires == 0 {
			c.Feeds[k].Expires = c.Expires
		}
	}

	ctx := context.Background()
	switch flag.Arg(0) {
	case "pull":
		for _, f := range c.Feeds {
			err = runPull(ctx, f)
			if err != nil {
				log.Println(err)
			}
		}
	case "clean":
		for _, f := range c.Feeds {
			err = runCleanByStatus(ctx, f, c.Clean.Status)
			if err != nil {
				log.Println(err)
			}
		}
	}
}
