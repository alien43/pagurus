package config_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/testutil"
)

func cfgWithEtcd(t *testing.T, etcd string) (*config.Config, error) {
	t.Helper()
	return config.Parse([]byte(`{"service": "s", "project_prefix": "p", "members": ["asus"], "start": {"kind": "instances"}, "etcd": ` + etcd + `}`))
}

func TestEtcdSecurityValidation(t *testing.T) {
	for name, etcd := range map[string]string{
		"cert without key":           `{"endpoints": ["x"], "cert": "c.pem"}`,
		"key without cert":           `{"endpoints": ["x"], "key": "k.pem"}`,
		"user without password_file": `{"endpoints": ["x"], "user": "root"}`,
		"password_file without user": `{"endpoints": ["x"], "password_file": "/pw"}`,
	} {
		if _, err := cfgWithEtcd(t, etcd); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestEtcdTLSClient: the config's client reaches a TLS-only etcd that
// requires client certificates, and is refused without the client cert.
func TestEtcdTLSClient(t *testing.T) {
	certs := testutil.NewCerts(t)
	url := testutil.EtcdTLS(t, certs)

	try := func(etcd string) error {
		c, err := cfgWithEtcd(t, etcd)
		if err != nil {
			t.Fatal(err)
		}
		cc, err := c.EtcdClientConfig()
		if err != nil {
			return err
		}
		cli, err := clientv3.New(cc)
		if err != nil {
			return err
		}
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err = cli.Put(ctx, "/pagurus-tls-probe", "x")
		return err
	}
	ok := `{"endpoints": ["` + url + `"], "dial_timeout": "2s", "cacert": "` + certs.CA + `", "cert": "` + certs.ClientCert + `", "key": "` + certs.ClientKey + `"}`
	if err := try(ok); err != nil {
		t.Fatalf("TLS with a client cert: %v", err)
	}
	caOnly := `{"endpoints": ["` + url + `"], "dial_timeout": "2s", "cacert": "` + certs.CA + `"}`
	if err := try(caOnly); err == nil {
		t.Fatal("no client cert, yet the put succeeded")
	}
}

func TestEtcdPasswordAndConchArgs(t *testing.T) {
	pw := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(pw, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := cfgWithEtcd(t, `{"endpoints": ["https://a:2379", "https://b:2379"], "dial_timeout": "3s", "cacert": "/ca.pem", "user": "pagurus", "password_file": "`+pw+`"}`)
	if err != nil {
		t.Fatal(err)
	}
	c.Etcd.CACert = "" // no CA file on disk here; the auth half is what this checks
	cc, err := c.EtcdClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cc.Username != "pagurus" || cc.Password != "s3cret" {
		t.Fatalf("auth %q/%q, want pagurus/s3cret (trailing newline trimmed)", cc.Username, cc.Password)
	}
	c.Etcd.CACert = "/ca.pem"
	got := strings.Join(c.ConchEtcdArgs(), " ")
	want := "--endpoints https://a:2379,https://b:2379 --dial-timeout 3s --cacert /ca.pem --user pagurus --password-file " + pw
	if got != want {
		t.Fatalf("conch args\n got %s\nwant %s", got, want)
	}
	if strings.Contains(got, "s3cret") {
		t.Fatal("the password itself reached the command line")
	}
}
