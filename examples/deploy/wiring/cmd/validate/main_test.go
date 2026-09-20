package main

import (
	"os"
	"strings"
	"testing"
)

func fixture(t *testing.T) (string, string) {
	t.Helper()
	manifest, err := os.ReadFile("../../../cloud-pooled.yaml")
	if err != nil {
		t.Fatal(err)
	}
	wiring, err := os.ReadFile("../../wiring.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(manifest), string(wiring)
}

func TestTemplateAndUnsafeMutations(t *testing.T) {
	manifest, wiring := fixture(t)
	if got := validate(manifest, wiring); len(got) != 0 {
		t.Fatalf("template: %v", got)
	}
	tests := []struct{ name, manifest, wiring, want string }{
		{"extra ingress", manifest + "\n---\napiVersion: networking.k8s.io/v1\nkind: Ingress\n", wiring, "unexpected Kubernetes resource"},
		{"extra configmap", manifest + "\n---\napiVersion: v1\nkind: ConfigMap\n", wiring, "unexpected Kubernetes resource"},
		{"secret", manifest + "\n---\napiVersion: v1\nkind: Secret\ndata: {password: c2VjcmV0}\n", wiring, "unexpected Kubernetes resource"},
		{"literal credential", strings.Replace(manifest, "ports:\n            -", "env:\n            - {name: DB_PASSWORD, value: leaked}\n          ports:\n            -", 1), wiring, "literal credential"},
		{"public HostLink", strings.Replace(manifest, "name: http, port: 8080", "name: hostlink, port: 8080", 1), wiring, "public HostLink"},
		{"external IP", strings.Replace(manifest, "type: ClusterIP", "type: ClusterIP\n  externalIPs: [203.0.113.7]", 1), wiring, "externalIPs"},
		{"host network", strings.Replace(manifest, "terminationGracePeriodSeconds: 120", "hostNetwork: true\n      terminationGracePeriodSeconds: 120", 1), wiring, "hostNetwork"},
		{"host port", strings.Replace(manifest, "containerPort: 8080", "containerPort: 8080, hostPort: 8080", 1), wiring, "hostPort"},
		{"missing memory limit", strings.Replace(manifest, `limits: {cpu: "2", memory: 8Gi}`, `limits: {cpu: "2"}`, 1), wiring, "missing limits memory"},
		{"short grace", strings.Replace(manifest, "terminationGracePeriodSeconds: 120", "terminationGracePeriodSeconds: 5", 1), wiring, "termination grace"},
		{"unbounded queue", manifest, strings.Replace(wiring, "PerConnectionQueueBytes = 1 << 20", "PerConnectionQueueBytes = 0", 1), "byte queue"},
		{"no encryption confirmation", manifest, strings.Replace(wiring, "RequireConfirmedEncryption: true", "RequireConfirmedEncryption: false", 1), "encryption confirmation"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(validate(tc.manifest, tc.wiring), "\n")
			if !strings.Contains(got, tc.want) {
				t.Fatalf("findings %q do not include %q", got, tc.want)
			}
		})
	}
}
