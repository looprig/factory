// Command validate checks the Factory-owned Kubernetes example before product adaptation.
package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v2"
)

var secretField = regexp.MustCompile(`(?i)password|secret.?key|access.?key|credential|token|dsn`)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: validate MANIFEST wiring.go")
		os.Exit(2)
	}
	manifest, err := os.ReadFile(os.Args[1]) // #nosec G703 -- the operator names the file to check
	if err != nil {
		panic(err)
	}
	wiring, err := os.ReadFile(os.Args[2]) // #nosec G703 -- the operator names the file to check
	if err != nil {
		panic(err)
	}
	findings := validate(string(manifest), string(wiring))
	if len(findings) > 0 {
		fmt.Fprintln(os.Stderr, strings.Join(findings, "\n"))
		os.Exit(1)
	}
	fmt.Println("Factory deployment example: structural checks passed")
}

func object(v any) map[interface{}]interface{} {
	m, _ := v.(map[interface{}]interface{})
	return m
}

func field(v any, path ...string) any {
	for _, key := range path {
		v = object(v)[key]
	}
	return v
}

func entries(v any) []any {
	s, _ := v.([]interface{})
	return s
}

func present(v any) bool {
	if v == nil {
		return false
	}
	if s, ok := v.(string); ok {
		return s != ""
	}
	return true
}

func secretLiterals(v any, findings *[]string) {
	switch x := v.(type) {
	case map[interface{}]interface{}:
		for k, value := range x {
			if key, ok := k.(string); ok && secretField.MatchString(key) && present(value) {
				if _, scalar := value.(string); scalar {
					*findings = append(*findings, "literal secret field "+key)
				}
			}
			secretLiterals(value, findings)
		}
	case []interface{}:
		for _, value := range x {
			secretLiterals(value, findings)
		}
	}
}

func validate(manifest, wiring string) []string {
	var findings []string
	decoder := yaml.NewDecoder(strings.NewReader(manifest))
	var deployment, service any
	count := 0
	for {
		var doc any
		err := decoder.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return []string{"invalid YAML: " + err.Error()}
		}
		if doc == nil {
			continue
		}
		count++
		secretLiterals(doc, &findings)
		switch field(doc, "kind") {
		case "Deployment":
			if deployment != nil {
				findings = append(findings, "duplicate Deployment")
			}
			deployment = doc
		case "Service":
			if service != nil {
				findings = append(findings, "duplicate Service")
			}
			service = doc
		default:
			findings = append(findings, "unexpected Kubernetes resource")
		}
	}
	if count != 2 || deployment == nil || service == nil {
		findings = append(findings, "expected exactly one Deployment and one Service")
		return findings
	}
	if field(deployment, "spec", "replicas") != 2 {
		findings = append(findings, "Factory replica count must be two")
	}
	pod := field(deployment, "spec", "template", "spec")
	if present(field(pod, "affinity")) {
		findings = append(findings, "Factory must not require affinity")
	}
	if field(pod, "hostNetwork") == true {
		findings = append(findings, "hostNetwork exposes the pod")
	}
	grace, _ := field(pod, "terminationGracePeriodSeconds").(int)
	if grace < 60 {
		findings = append(findings, "termination grace below 60 seconds")
	}
	containers := entries(field(pod, "containers"))
	if len(containers) != 1 {
		findings = append(findings, "expected one Factory container")
	}
	for _, container := range containers {
		for _, kind := range []string{"requests", "limits"} {
			for _, resource := range []string{"cpu", "memory"} {
				if !present(field(container, "resources", kind, resource)) {
					findings = append(findings, "missing "+kind+" "+resource+" bound")
				}
			}
		}
		for _, env := range entries(field(container, "env")) {
			name, _ := field(env, "name").(string)
			if secretField.MatchString(name) && present(field(env, "value")) {
				findings = append(findings, "literal credential in env "+name)
			}
		}
		for _, port := range entries(field(container, "ports")) {
			name, _ := field(port, "name").(string)
			if strings.Contains(strings.ToLower(name), "hostlink") {
				findings = append(findings, "HostLink pod port")
			}
			if present(field(port, "hostPort")) {
				findings = append(findings, "hostPort exposes the pod")
			}
		}
	}
	if field(service, "spec", "type") != "ClusterIP" {
		findings = append(findings, "Service must be ClusterIP")
	}
	if len(entries(field(service, "spec", "externalIPs"))) > 0 {
		findings = append(findings, "Service externalIPs expose Factory")
	}
	for _, port := range entries(field(service, "spec", "ports")) {
		name, _ := field(port, "name").(string)
		if strings.Contains(strings.ToLower(name), "hostlink") {
			findings = append(findings, "public HostLink exposure")
		}
	}
	for _, check := range []struct{ pattern, message string }{
		{`PerConnectionQueueBytes\s*=\s*1\s*<<\s*20`, "missing finite per-connection byte queue"},
		{`MaxConnections\s*=\s*maxConnections`, "missing per-replica admission bound"},
		{`MaxConcurrentTransfers\s*:\s*[1-9][0-9]*`, "missing S3 transfer bound"},
		{`RequireConfirmedEncryption\s*:\s*true`, "S3 encryption confirmation missing"},
		{`Encryption\s*:\s*s3store\.EncryptionKMS`, "S3 KMS encryption missing"},
		{`Migrations\s*:\s*pgstore\.MigrationValidate`, "replicas must validate migrations"},
		{`DeploymentPrefix\s*:\s*cfg\.DeploymentPrefix`, "shared S3 deployment prefix missing"},
	} {
		if !regexp.MustCompile(check.pattern).MatchString(wiring) {
			findings = append(findings, check.message)
		}
	}
	return findings
}
