// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"golang.org/x/net/context"

	"gopkg.in/olivere/elastic.v5/uritemplates"
)

// Search for documents in Elasticsearch.
type SearchWithClusters struct {
	client            *Client
	searchSource      *SearchSource
	source            interface{}
	pretty            bool
	filterPath        []string
	searchType        string
	index             []string
	typ               []string
	routing           string
	preference        string
	requestCache      *bool
	ignoreUnavailable *bool
	allowNoIndices    *bool
	expandWildcards   string
}

// NewSearchWithClusteringService creates a new service for searching in Elasticsearch.
func NewSearchWithClustersService(client *Client) *SearchWithClusters {
	builder := &SearchWithClusters{
		client:       client,
		searchSource: NewSearchSource(),
	}
	return builder
}

// SearchSource sets the search source builder to use with this service.
func (s *SearchWithClusters) SearchSource(searchSource *SearchSource) *SearchWithClusters {
	s.searchSource = searchSource
	if s.searchSource == nil {
		s.searchSource = NewSearchSource()
	}
	return s
}

// Source allows the user to set the request body manually without using
// any of the structs and interfaces in Elastic.
func (s *SearchWithClusters) Source(source interface{}) *SearchWithClusters {
	s.source = source
	return s
}

// FilterPath allows reducing the response, a mechanism known as
// response filtering and described here:
// https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#common-options-response-filtering.
func (s *SearchWithClusters) FilterPath(filterPath ...string) *SearchWithClusters {
	s.filterPath = append(s.filterPath, filterPath...)
	return s
}

// Index sets the names of the indices to use for search.
func (s *SearchWithClusters) Index(index ...string) *SearchWithClusters {
	s.index = append(s.index, index...)
	return s
}

// Types adds search restrictions for a list of types.
func (s *SearchWithClusters) Type(typ ...string) *SearchWithClusters {
	s.typ = append(s.typ, typ...)
	return s
}

// Pretty enables the caller to indent the JSON output.
func (s *SearchWithClusters) Pretty(pretty bool) *SearchWithClusters {
	s.pretty = pretty
	return s
}

// Timeout sets the timeout to use, e.g. "1s" or "1000ms".
func (s *SearchWithClusters) Timeout(timeout string) *SearchWithClusters {
	s.searchSource = s.searchSource.Timeout(timeout)
	return s
}

// TimeoutInMillis sets the timeout in milliseconds.
func (s *SearchWithClusters) TimeoutInMillis(timeoutInMillis int) *SearchWithClusters {
	s.searchSource = s.searchSource.TimeoutInMillis(timeoutInMillis)
	return s
}

// SearchType sets the search operation type. Valid values are:
// "query_then_fetch", "query_and_fetch", "dfs_query_then_fetch",
// "dfs_query_and_fetch", "count", "scan".
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html
// for details.
func (s *SearchWithClusters) SearchType(searchType string) *SearchWithClusters {
	s.searchType = searchType
	return s
}

// Routing is a list of specific routing values to control the shards
// the search will be executed on.
func (s *SearchWithClusters) Routing(routings ...string) *SearchWithClusters {
	s.routing = strings.Join(routings, ",")
	return s
}

// Preference sets the preference to execute the search. Defaults to
// randomize across shards ("random"). Can be set to "_local" to prefer
// local shards, "_primary" to execute on primary shards only,
// or a custom value which guarantees that the same order will be used
// across different requests.
func (s *SearchWithClusters) Preference(preference string) *SearchWithClusters {
	s.preference = preference
	return s
}

// RequestCache indicates whether the cache should be used for this
// request or not, defaults to index level setting.
func (s *SearchWithClusters) RequestCache(requestCache bool) *SearchWithClusters {
	s.requestCache = &requestCache
	return s
}

// Query sets the query to perform, e.g. MatchAllQuery.
func (s *SearchWithClusters) Query(query Query) *SearchWithClusters {
	s.searchSource = s.searchSource.Query(query)
	return s
}

// PostFilter will be executed after the query has been executed and
// only affects the search hits, not the aggregations.
// This filter is always executed as the last filtering mechanism.
func (s *SearchWithClusters) PostFilter(postFilter Query) *SearchWithClusters {
	s.searchSource = s.searchSource.PostFilter(postFilter)
	return s
}

// FetchSource indicates whether the response should contain the stored
// _source for every hit.
func (s *SearchWithClusters) FetchSource(fetchSource bool) *SearchWithClusters {
	s.searchSource = s.searchSource.FetchSource(fetchSource)
	return s
}

// FetchSourceContext indicates how the _source should be fetched.
func (s *SearchWithClusters) FetchSourceContext(fetchSourceContext *FetchSourceContext) *SearchWithClusters {
	s.searchSource = s.searchSource.FetchSourceContext(fetchSourceContext)
	return s
}

// Highlight adds highlighting to the search.
func (s *SearchWithClusters) Highlight(highlight *Highlight) *SearchWithClusters {
	s.searchSource = s.searchSource.Highlight(highlight)
	return s
}

// GlobalSuggestText defines the global text to use with all suggesters.
// This avoids repetition.
func (s *SearchWithClusters) GlobalSuggestText(globalText string) *SearchWithClusters {
	s.searchSource = s.searchSource.GlobalSuggestText(globalText)
	return s
}

// Suggester adds a suggester to the search.
func (s *SearchWithClusters) Suggester(suggester Suggester) *SearchWithClusters {
	s.searchSource = s.searchSource.Suggester(suggester)
	return s
}

// Aggregation adds an aggreation to perform as part of the search.
func (s *SearchWithClusters) Aggregation(name string, aggregation Aggregation) *SearchWithClusters {
	s.searchSource = s.searchSource.Aggregation(name, aggregation)
	return s
}

// MinScore sets the minimum score below which docs will be filtered out.
func (s *SearchWithClusters) MinScore(minScore float64) *SearchWithClusters {
	s.searchSource = s.searchSource.MinScore(minScore)
	return s
}

// From index to start the search from. Defaults to 0.
func (s *SearchWithClusters) From(from int) *SearchWithClusters {
	s.searchSource = s.searchSource.From(from)
	return s
}

// Size is the number of search hits to return. Defaults to 10.
func (s *SearchWithClusters) Size(size int) *SearchWithClusters {
	s.searchSource = s.searchSource.Size(size)
	return s
}

// Explain indicates whether each search hit should be returned with
// an explanation of the hit (ranking).
func (s *SearchWithClusters) Explain(explain bool) *SearchWithClusters {
	s.searchSource = s.searchSource.Explain(explain)
	return s
}

// Version indicates whether each search hit should be returned with
// a version associated to it.
func (s *SearchWithClusters) Version(version bool) *SearchWithClusters {
	s.searchSource = s.searchSource.Version(version)
	return s
}

// Sort adds a sort order.
func (s *SearchWithClusters) Sort(field string, ascending bool) *SearchWithClusters {
	s.searchSource = s.searchSource.Sort(field, ascending)
	return s
}

// SortWithInfo adds a sort order.
func (s *SearchWithClusters) SortWithInfo(info SortInfo) *SearchWithClusters {
	s.searchSource = s.searchSource.SortWithInfo(info)
	return s
}

// SortBy	adds a sort order.
func (s *SearchWithClusters) SortBy(sorter ...Sorter) *SearchWithClusters {
	s.searchSource = s.searchSource.SortBy(sorter...)
	return s
}

// NoStoredFields indicates that no stored fields should be loaded, resulting in only
// id and type to be returned per field.
func (s *SearchWithClusters) NoStoredFields() *SearchWithClusters {
	s.searchSource = s.searchSource.NoStoredFields()
	return s
}

// StoredField adds a single field to load and return (note, must be stored) as
// part of the search request. If none are specified, the source of the
// document will be returned.
func (s *SearchWithClusters) StoredField(fieldName string) *SearchWithClusters {
	s.searchSource = s.searchSource.StoredField(fieldName)
	return s
}

// StoredFields	sets the fields to load and return as part of the search request.
// If none are specified, the source of the document will be returned.
func (s *SearchWithClusters) StoredFields(fields ...string) *SearchWithClusters {
	s.searchSource = s.searchSource.StoredFields(fields...)
	return s
}

// IgnoreUnavailable indicates whether the specified concrete indices
// should be ignored when unavailable (missing or closed).
func (s *SearchWithClusters) IgnoreUnavailable(ignoreUnavailable bool) *SearchWithClusters {
	s.ignoreUnavailable = &ignoreUnavailable
	return s
}

// AllowNoIndices indicates whether to ignore if a wildcard indices
// expression resolves into no concrete indices. (This includes `_all` string
// or when no indices have been specified).
func (s *SearchWithClusters) AllowNoIndices(allowNoIndices bool) *SearchWithClusters {
	s.allowNoIndices = &allowNoIndices
	return s
}

// ExpandWildcards indicates whether to expand wildcard expression to
// concrete indices that are open, closed or both.
func (s *SearchWithClusters) ExpandWildcards(expandWildcards string) *SearchWithClusters {
	s.expandWildcards = expandWildcards
	return s
}

// buildURL builds the URL for the operation.
func (s *SearchWithClusters) buildURL() (string, url.Values, error) {
	var err error
	var path string

	if len(s.index) > 0 && len(s.typ) > 0 {
		path, err = uritemplates.Expand("/{index}/{type}/_search_with_clusters", map[string]string{
			"index": strings.Join(s.index, ","),
			"type":  strings.Join(s.typ, ","),
		})
	} else if len(s.index) > 0 {
		path, err = uritemplates.Expand("/{index}/_search_with_clusters", map[string]string{
			"index": strings.Join(s.index, ","),
		})
	} else if len(s.typ) > 0 {
		path, err = uritemplates.Expand("/_all/{type}/_search_with_clusters", map[string]string{
			"type": strings.Join(s.typ, ","),
		})
	} else {
		path = "/_search_with_clusters"
	}
	if err != nil {
		return "", url.Values{}, err
	}

	// Add query string parameters
	params := url.Values{}
	if s.pretty {
		params.Set("pretty", fmt.Sprintf("%v", s.pretty))
	}
	if s.searchType != "" {
		params.Set("search_type", s.searchType)
	}
	if s.routing != "" {
		params.Set("routing", s.routing)
	}
	if s.preference != "" {
		params.Set("preference", s.preference)
	}
	if s.requestCache != nil {
		params.Set("request_cache", fmt.Sprintf("%v", *s.requestCache))
	}
	if s.allowNoIndices != nil {
		params.Set("allow_no_indices", fmt.Sprintf("%v", *s.allowNoIndices))
	}
	if s.expandWildcards != "" {
		params.Set("expand_wildcards", s.expandWildcards)
	}
	if s.ignoreUnavailable != nil {
		params.Set("ignore_unavailable", fmt.Sprintf("%v", *s.ignoreUnavailable))
	}
	if len(s.filterPath) > 0 {
		params.Set("filter_path", strings.Join(s.filterPath, ","))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *SearchWithClusters) Validate() error {
	return nil
}

// Do executes the search and returns a SearchWithClustersResult.
func (s *SearchWithClusters) Do(ctx context.Context) (*SearchWithClustersResult, error) {
	// Check pre-conditions
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}

	// Perform request
	var body interface{}
	if s.source != nil {
		body = s.source
	} else {
		src, err := s.searchSource.Source()
		if err != nil {
			return nil, err
		}
		body = src
	}

	res, err := s.client.PerformRequest(ctx, "POST", path, params, body)
	if err != nil {
		return nil, err
	}

	// Return search results
	ret := new(SearchWithClustersResult)
	if err := s.client.decoder.Decode(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// SearchWithClustersResult is the result of a search in Elasticsearch.
type SearchWithClustersResult struct {
	TookInMillis int64         `json:"took"`            // search time in milliseconds
	Hits         *SearchHits   `json:"hits"`            // the actual search hits
	TimedOut     bool          `json:"timed_out"`       // true if the search timed out
	Clusters     []*json.RawMessage `json:"clusters"`   // carrot2 clusters
														//Error        string        `json:"error,omitempty"` // used in MultiSearch only
														// TODO double-check that MultiGet now returns details error information
	Error        *ErrorDetails `json:"error,omitempty"` // only used in MultiGet
}

// TotalHits is a convenience function to return the number of hits for
// a search result.
func (r *SearchWithClustersResult) TotalHits() int64 {
	if r.Hits != nil {
		return r.Hits.TotalHits
	}
	return 0
}

// Each is a utility function to iterate over all hits. It saves you from
// checking for nil values. Notice that Each will ignore errors in
// serializing JSON.
func (r *SearchWithClustersResult) Each(typ reflect.Type) []interface{} {
	if r.Hits == nil || r.Hits.Hits == nil || len(r.Hits.Hits) == 0 {
		return nil
	}
	var slice []interface{}
	for _, hit := range r.Hits.Hits {
		v := reflect.New(typ).Elem()
		if err := json.Unmarshal(*hit.Source, v.Addr().Interface()); err == nil {
			slice = append(slice, v.Interface())
		}
	}
	return slice
}


