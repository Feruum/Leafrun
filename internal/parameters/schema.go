// Package parameters validates already-normalized parameters against schemas
// in an immutable base snapshot. It never injects defaults or mutates the tree.
package parameters

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"strings"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const SchemaResourceLimit int64 = 4 << 20
const draft2020 = "https://json-schema.org/draft/2020-12/schema"

// Validate consumes exact normalized parameter bytes saved at admission. The
// schema and refs are loaded from base before input.json is overlaid. Schema
// identifiers resolve only in leafrun://snapshot/; there is no network/file URL
// loader. Standard metaschemas are supplied by jsonschema's embedded resources.
func Validate(ctx context.Context, base fs.FS, schemaPath string, parameters []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if base == nil || !generationv1.LogicalFilePath(schemaPath) {
		return errors.New("schema requires an immutable base and logical file path")
	}
	if len(parameters) > 1<<20 {
		return errors.New("parameters exceed 1 MiB")
	}
	value, err := jsonvalue.Decode(parameters, 1<<20)
	if err != nil {
		return fmt.Errorf("parameters JSON: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return errors.New("parameters must be an object")
	}
	loader := &snapshotLoader{ctx: ctx, base: base, resources: map[string]any{}}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(loader)
	schemaURL := (&url.URL{Scheme: "leafrun", Host: "snapshot", Path: "/" + schemaPath}).String()
	compiled, err := compiler.Compile(schemaURL)
	if err != nil {
		return fmt.Errorf("schema validation: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = compiled.Validate(value); err != nil {
		return fmt.Errorf("parameters validation: %w", err)
	}
	return ctx.Err()
}

type snapshotLoader struct {
	ctx       context.Context
	base      fs.FS
	resources map[string]any
	total     int64
}

func (l *snapshotLoader) Load(location string) (any, error) {
	if err := l.ctx.Err(); err != nil {
		return nil, err
	}
	u, err := closedURL(location)
	if err != nil {
		return nil, err
	}
	name := strings.TrimPrefix(u.Path, "/")
	if !generationv1.LogicalFilePath(name) {
		return nil, errors.New("schema reference is not a logical snapshot file")
	}
	if cached, ok := l.resources[name]; ok {
		return cached, nil
	}
	file, err := l.base.Open(name)
	if err != nil {
		return nil, fmt.Errorf("schema resource %q: %w", name, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > SchemaResourceLimit-l.total {
		return nil, errors.New("schema resource budget exceeded or non-file resource")
	}
	data, err := io.ReadAll(io.LimitReader(file, SchemaResourceLimit-l.total+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > SchemaResourceLimit-l.total {
		return nil, errors.New("schema resource budget exceeded")
	}
	l.total += int64(len(data))
	resource, err := jsonvalue.Decode(data, SchemaResourceLimit)
	if err != nil {
		return nil, err
	}
	if err = checkSchemaSpace(resource, u); err != nil {
		return nil, err
	}
	l.resources[name] = resource
	return resource, l.ctx.Err()
}
func closedURL(location string) (*url.URL, error) {
	u, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "leafrun" || u.Host != "snapshot" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || !strings.HasPrefix(u.Path, "/") {
		return nil, errors.New("schema URI leaves the immutable snapshot namespace")
	}
	return u, nil
}

// Walk only schema-bearing keywords. Annotation/default/example data may itself
// contain keys such as $id and must remain ordinary data. Custom metaschemas are
// excluded so a format-assertion vocabulary cannot change annotation semantics.
func checkSchemaSpace(value any, base *url.URL) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if schema, exists := obj["$schema"]; exists {
		if schema != draft2020 {
			return errors.New("only standard Draft 2020-12 is supported")
		}
	}
	if raw, exists := obj["$id"]; exists {
		id, ok := raw.(string)
		if !ok {
			return errors.New("schema $id must be a URI string")
		}
		ref, err := url.Parse(id)
		if err != nil {
			return err
		}
		resolved := base.ResolveReference(ref)
		base, err = closedURL(resolved.String())
		if err != nil {
			return err
		}
	}
	for _, keyword := range []string{"additionalProperties", "unevaluatedProperties", "propertyNames", "contains", "not", "if", "then", "else", "items", "unevaluatedItems", "contentSchema"} {
		if child, exists := obj[keyword]; exists {
			if err := checkSchemaSpace(child, base); err != nil {
				return err
			}
		}
	}
	for _, keyword := range []string{"$defs", "definitions", "properties", "patternProperties", "dependentSchemas"} {
		if children, ok := obj[keyword].(map[string]any); ok {
			for _, child := range children {
				if err := checkSchemaSpace(child, base); err != nil {
					return err
				}
			}
		}
	}
	for _, keyword := range []string{"allOf", "anyOf", "oneOf", "prefixItems"} {
		if children, ok := obj[keyword].([]any); ok {
			for _, child := range children {
				if err := checkSchemaSpace(child, base); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
