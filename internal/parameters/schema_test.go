package parameters

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSchemaExactNumbersAndAnnotations(t *testing.T) {
	tree := fstest.MapFS{"schema.json": {Data: []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"n":{"type":"integer","minimum":9007199254740993},"email":{"type":"string","format":"email"},"defaulted":{"default":"not-inserted"}},"required":["n"]}`)}}
	input := []byte(`{"email":"not-an-email","n":9007199254740993}`)
	original := string(input)
	if err := Validate(context.Background(), tree, "schema.json", input); err != nil {
		t.Fatal(err)
	}
	if string(input) != original {
		t.Fatal("parameters changed")
	}
	if err := Validate(context.Background(), tree, "schema.json", []byte(`{"n":9007199254740992}`)); err == nil {
		t.Fatal("rounded below minimum")
	}
	if err := Validate(context.Background(), tree, "schema.json", []byte(`{"n":"9007199254740993"}`)); err == nil {
		t.Fatal("coerced string")
	}
}
func TestSchemaIDAndRelativeRefsUseImmutableTree(t *testing.T) {
	tree := fstest.MapFS{
		"schemas/main.json": {Data: []byte(`{"$id":"nested/base.json","$ref":"../defs.json"}`)},
		"schemas/defs.json": {Data: []byte(`{"type":"object","properties":{"x":{"const":1.0}},"required":["x"]}`)},
		"input.json":        {Data: []byte(`{"type":"object","properties":{"before":{"const":true}},"required":["before"]}`)},
	}
	if err := Validate(context.Background(), tree, "schemas/main.json", []byte(`{"x":1.0}`)); err != nil {
		t.Fatal(err)
	}
	if err := Validate(context.Background(), tree, "input.json", []byte(`{"before":true}`)); err != nil {
		t.Fatal(err)
	}
	if string(tree["input.json"].Data) != `{"type":"object","properties":{"before":{"const":true}},"required":["before"]}` {
		t.Fatal("mutated base schema")
	}
}
func TestSchemaClosedResolverAndDraft(t *testing.T) {
	for _, schema := range []string{
		`{"$ref":"https://127.0.0.1:1/canary"}`, `{"$ref":"file:///etc/passwd"}`, `{"$id":"https://evil.test/base.json","type":"object"}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#"}`, `{"$schema":"https://evil.test/meta.json"}`,
		`{"properties":{"x":{"$id":"file:///tmp/canary"}}}`, `{"type":17}`,
		`{"type":"object","type":"string"}`, `{"description":"\ud800","type":"object"}`,
	} {
		tree := fstest.MapFS{"schema.json": {Data: []byte(schema)}}
		if err := Validate(context.Background(), tree, "schema.json", []byte(`{}`)); err == nil {
			t.Errorf("accepted forbidden/invalid schema %s", schema)
		}
	}
	tree := fstest.MapFS{"schema.json": {Data: []byte(`{"type":"object","properties":{"x":{"default":{"$id":"https://ordinary-data.test/"}}}}`)}}
	if err := Validate(context.Background(), tree, "schema.json", []byte(`{}`)); err != nil {
		t.Fatal("annotation data treated as schema", err)
	}
}
func TestSchemaResourcesBoundedAndCanceled(t *testing.T) {
	tree := fstest.MapFS{"schema.json": {Data: []byte(`{"description":"` + strings.Repeat("a", 2<<20) + `","$ref":"defs.json"}`)}, "defs.json": {Data: []byte(`{"description":"` + strings.Repeat("b", 2<<20) + `","type":"object"}`)}}
	if err := Validate(context.Background(), tree, "schema.json", []byte(`{}`)); err == nil {
		t.Fatal("accepted total resources above 4 MiB")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Validate(ctx, tree, "schema.json", []byte(`{}`)); err == nil {
		t.Fatal("ignored cancellation")
	}
	if err := Validate(context.Background(), tree, "../schema.json", []byte(`{}`)); err == nil {
		t.Fatal("native/escaping initial schema path")
	}
}
