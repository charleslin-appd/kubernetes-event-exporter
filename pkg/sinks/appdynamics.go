package sinks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/opsgenie/kubernetes-event-exporter/pkg/kube"
)

type AppDynamicsConfig struct {
	Endpoint string                 `yaml:"endpoint"`
	TLS      TLS                    `yaml:"tls"`
	Layout   map[string]interface{} `yaml:"layout"`
	Headers  map[string]string      `yaml:"headers"`
}

func NewAppDynamicsSink(cfg *AppDynamicsConfig) (Sink, error) {
	return &AppDynamicsSink{cfg: cfg}, nil
}

type AppDynamicsSink struct {
	cfg *AppDynamicsConfig
}

func (w *AppDynamicsSink) Close() {
	// No-op
}

func (w *AppDynamicsSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	serializedLayout, err := serializeEventWithLayout(w.cfg.Layout, ev)
	if err != nil {
		return err
	}

	// AppDynamics analytics requires a json array as input
	var reqBody bytes.Buffer
	reqBody.WriteString("[")
	reqBody.Write(serializedLayout)
	reqBody.WriteString("]")

	req, err := http.NewRequest(http.MethodPost, w.cfg.Endpoint, bytes.NewReader(reqBody.Bytes()))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/vnd.appd.events+json;v=2")
	for k, v := range w.cfg.Headers {
		req.Header.Add(k, v)
	}
	tlsClientConfig, err := setupTLS(&w.cfg.TLS)
	if err != nil {
		return fmt.Errorf("failed to setup TLS: %w", err)
	}
	client := http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: tlsClientConfig,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return errors.New("not successfull (2xx) response: " + string(body))
	}

	return nil
}
