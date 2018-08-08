package finder

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type taggedTermOp int

const (
	taggedTermEq       taggedTermOp = 1
	taggedTermMatch    taggedTermOp = 2
	taggedTermNe       taggedTermOp = 3
	taggedTermNotMatch taggedTermOp = 4
)

type taggedTerm struct {
	key   string
	op    taggedTermOp
	value string
}

type taggedTermList []taggedTerm

func (s taggedTermList) Len() int {
	return len(s)
}
func (s taggedTermList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s taggedTermList) Less(i, j int) bool {
	return s[i].op < s[j].op
}

type TaggedFinder struct {
	url   string             // clickhouse dsn
	table string             // graphite_tag table
	opts  clickhouse.Options // clickhouse query timeout
	body  []byte             // clickhouse response
}

func NewTagged(url string, table string, opts clickhouse.Options) *TaggedFinder {
	return &TaggedFinder{
		url:   url,
		table: table,
		opts:  opts,
	}
}

func taggedTermWhere1(term *taggedTerm) string {
	switch term.op {
	case taggedTermEq:
		return fmt.Sprintf("Tag1=%s", Q(fmt.Sprintf("%s=%s", term.key, term.value)))
	case taggedTermNe:
		return fmt.Sprintf("Tag1!=%s", Q(fmt.Sprintf("%s=%s", term.key, term.value)))
	case taggedTermMatch:
		return fmt.Sprintf(
			"(Tag1 LIKE %s) AND (match(Tag1, %s))",
			Q(fmt.Sprintf("%s=%%", term.key)),
			Q(fmt.Sprintf("%s=%s", term.key, term.value)),
		)

	case taggedTermNotMatch:
		return fmt.Sprintf(
			"NOT ((Tag1 LIKE %s) AND (match(Tag1, %s)))",
			Q(fmt.Sprintf("%s=%%", term.key)),
			Q(fmt.Sprintf("%s=%s", term.key, term.value)),
		)
	default:
		return ""
	}
}

func taggedTermWhereN(term *taggedTerm) string {
	// arrayExists((x) -> %s, Tags)
	switch term.op {
	case taggedTermEq:
		return fmt.Sprintf("arrayExists((x) -> x=%s, Tags)", Q(fmt.Sprintf("%s=%s", term.key, term.value)))
	case taggedTermNe:
		return fmt.Sprintf("NOT arrayExists((x) -> x=%s, Tags)", Q(fmt.Sprintf("%s=%s", term.key, term.value)))
	case taggedTermMatch:
		return fmt.Sprintf(
			"arrayExists((x) -> (x LIKE %s) AND (match(x, %s)), Tags)",
			Q(fmt.Sprintf("%s=%%", term.key)),
			Q(fmt.Sprintf("%s=%s", term.key, term.value)),
		)

	case taggedTermNotMatch:
		return fmt.Sprintf(
			"NOT arrayExists((x) -> (x LIKE %s) AND (match(x, %s)), Tags)",
			Q(fmt.Sprintf("%s=%%", term.key)),
			Q(fmt.Sprintf("%s=%s", term.key, term.value)),
		)
	default:
		return ""
	}
}

func ParseTerms(expr []string) ([]taggedTerm, error) {
	terms := make([]taggedTerm, len(expr))

	for i := 0; i < len(expr); i++ {
		s := expr[i]

		a := strings.SplitN(s, "=", 2)
		if len(a) != 2 {
			return []taggedTerm{}, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}

		a[0] = strings.TrimSpace(a[0])
		a[1] = strings.TrimSpace(a[1])

		op := "="

		if len(a[0]) > 0 && a[0][len(a[0])-1] == '!' {
			op = "!" + op
			a[0] = strings.TrimSpace(a[0][:len(a[0])-1])
		}

		if len(a[1]) > 0 && a[1][0] == '~' {
			op = op + "~"
			a[1] = strings.TrimSpace(a[1][1:])
		}

		terms[i].key = a[0]
		terms[i].value = a[1]

		if terms[i].key == "name" {
			terms[i].key = "__name__"
		}

		switch op {
		case "=":
			terms[i].op = taggedTermEq
		case "!=":
			terms[i].op = taggedTermNe
		case "=~":
			terms[i].op = taggedTermMatch
		case "!=~":
			terms[i].op = taggedTermNotMatch
		default:
			return []taggedTerm{}, fmt.Errorf("wrong seriesByTag expr: %#v", s)
		}
	}

	sort.Sort(taggedTermList(terms))

	return terms, nil
}

func (t *TaggedFinder) swapRarestTerm(ctx context.Context, terms *[]taggedTerm, from int64, until int64) error {
	var err error

	tagsToCompare := 0
	tag1Where := NewWhere()
	for _, term := range *terms {
		if term.op == taggedTermEq {
			tag1Where.Or(fmt.Sprintf("Tag1 = '%s=%s'", term.key, term.value))
			tagsToCompare++
		}
	}

	if tagsToCompare < 2 {
		return nil
	}

	dateWhere := makeDateWhere(from, until)

	sql := fmt.Sprintf("SELECT Tag1, count() FROM %s WHERE (%s) AND (%s) GROUP BY Tag1 ORDER BY count() LIMIT 1",
		t.table, dateWhere, tag1Where)
	body, err := clickhouse.Query(ctx, t.url, sql, t.table, t.opts)
	if err != nil {
		return err
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) == 0 {
		return nil
	}
	cols := strings.Split(rows[0], "\t")
	rarestTag := strings.Split(cols[0], "=")[0]

	for i := 1; i < len(*terms); i++ {
		if (*terms)[i].key == rarestTag {
			(*terms)[0], (*terms)[i] = (*terms)[i], (*terms)[0]
			break
		}
	}

	return nil
}

func MakeTaggedWhere(terms []taggedTerm) string {
	w := NewWhere()
	w.And(taggedTermWhere1(&terms[0]))

	for i := 1; i < len(terms); i++ {
		w.And(taggedTermWhereN(&terms[i]))
	}

	return w.String()
}

func makeDateWhere(from int64, until int64) string {
	dateWhere := NewWhere()

	dateWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(from, 0).Format("2006-01-02"),
		time.Unix(until, 0).Format("2006-01-02"),
	)

	return dateWhere.String()
}

func (t *TaggedFinder) makeWhere(ctx context.Context, query string, from int64, until int64) (string, error) {
	expr, _, err := parser.ParseExpr(query)
	if err != nil {
		return "", err
	}

	validationError := fmt.Errorf("wrong seriesByTag call: %#v", query)

	// check
	if !expr.IsFunc() {
		return "", validationError
	}
	if expr.Target() != "seriesByTag" {
		return "", validationError
	}

	args := expr.Args()
	if len(args) < 1 {
		return "", validationError
	}

	for i := 0; i < len(args); i++ {
		if !args[i].IsString() {
			return "", validationError
		}
	}

	conditions := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		s := args[i].StringValue()
		if s == "" {
			continue
		}
		conditions = append(conditions, s)
	}

	terms, err := ParseTerms(conditions)
	if err != nil {
		return "", err
	}

	err = t.swapRarestTerm(ctx, &terms, from, until)
	if err != nil {
		return "", err
	}

	taggedWhere := MakeTaggedWhere(terms)

	dateWhere := makeDateWhere(from, until)

	return fmt.Sprintf("%s AND %s", dateWhere, taggedWhere), nil
}

func (t *TaggedFinder) Execute(ctx context.Context, query string, from int64, until int64) error {
	w, err := t.makeWhere(ctx, query, from, until)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path HAVING argMax(Deleted, Version)==0", t.table, w)
	t.body, err = clickhouse.Query(ctx, t.url, sql, t.table, t.opts)
	return err
}

func (t *TaggedFinder) List() [][]byte {
	if t.body == nil {
		return [][]byte{}
	}

	rows := bytes.Split(t.body, []byte{'\n'})

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}

	rows = rows[:len(rows)-skip]

	return rows
}

func (t *TaggedFinder) Series() [][]byte {
	return t.List()
}

func (t *TaggedFinder) Abs(v []byte) []byte {
	u, err := url.Parse(string(v))
	if err != nil {
		return v
	}

	tags := make([]string, 0, len(u.Query()))
	for k, v := range u.Query() {
		tags = append(tags, fmt.Sprintf("%s=%s", k, v[0]))
	}

	sort.Strings(tags)
	if len(tags) == 0 {
		return []byte(u.Path)
	}

	return []byte(fmt.Sprintf("%s;%s", u.Path, strings.Join(tags, ";")))
}
