// Copyright (c) Jeevanandam M. (https://github.com/jeevatkm)
// Source code and usage is governed by a MIT style
// license that can be found in the LICENSE file.

package memcache

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"aahframe.work/aah/cache"
	"aahframe.work/aah/config"
	"aahframe.work/aah/log"
	"github.com/stretchr/testify/assert"
)

func TestMemcache(t *testing.T) {
	mgr := createCacheMgr(t, "memcache1", `
	cache {
		memcache1 {
			provider = "memcache"
			addresses = ["localhost:11211"]
			max_idle_conns = 4
			timeout = "5s"
		}
	}
`)

	e := mgr.CreateCache(&cache.Config{Name: "cache1", ProviderName: "memcache1"})
	assert.Nil(t, e, "unable to create cache")
	c := mgr.Cache("cache1")

	type sample struct {
		Name    string
		Present bool
		Value   string
	}

	testcases := []struct {
		label string
		key   string
		value interface{}
	}{
		{
			label: "Memcache integer",
			key:   "key1",
			value: 342348347,
		},
		{
			label: "Memcache float",
			key:   "key2",
			value: 0.78346374,
		},
		{
			label: "Memcache string",
			key:   "key3",
			value: "This is mt cache string",
		},
		{
			label: "Memcache map",
			key:   "key4",
			value: map[string]interface{}{"key1": 343434, "key2": "kjdhdsjkdhjs", "key3": 87235.3465},
		},
		{
			label: "Memcache struct",
			key:   "key5",
			value: sample{Name: "Jeeva", Present: true, Value: "memcache provider"},
		},
	}

	err := c.Put("pre-test-key1", sample{Name: "Jeeva", Present: true, Value: "memcache provider"}, 3*time.Second)
	assert.Equal(t, errors.New("aah/cache/cache1: gob: type not registered for interface: memcache.sample"), err)
	_, _ = c.GetOrPut("pre-test-key1", sample{Name: "Jeeva", Present: true, Value: "memcache provider"}, 3*time.Second)

	gob.Register(map[string]interface{}{})
	gob.Register(sample{})

	for _, tc := range testcases {
		t.Run(tc.label, func(t *testing.T) {
			assert.False(t, c.Exists(tc.key))
			assert.Nil(t, c.Get(tc.key))

			err := c.Put(tc.key, tc.value, 3*time.Second)
			assert.Nil(t, err)

			v := c.Get(tc.key)
			assert.Equal(t, tc.value, v)
			assert.True(t, c.Exists(tc.key))

			assert.Nil(t, c.Delete(tc.key))
			v, err = c.GetOrPut(tc.key, tc.value, 3*time.Second)
			assert.Nil(t, err)
			assert.Equal(t, tc.value, v)
		})
	}

	c.Flush()
}

func TestMemcacheAddAndGet(t *testing.T) {
	c := createTestCache(t, "memcache1", `
	cache {
		memcache1 {
			provider = "memcache"
			addresses = ["localhost:11211"]
		}
	}
`, &cache.Config{Name: "addgetcache", ProviderName: "memcache1"})

	for i := 0; i < 20; i++ {
		c.Put(fmt.Sprintf("key_%v", i), i, 3*time.Second)
	}

	for i := 5; i < 10; i++ {
		v := c.Get(fmt.Sprintf("key_%v", i))
		assert.Equal(t, i, v)
	}
	assert.Equal(t, "addgetcache", c.Name())
}

func TestMemcacheMultipleCache(t *testing.T) {
	mgr := createCacheMgr(t, "memcache1", `
	cache {
		memcache1 {
			provider = "memcache"
			addresses = ["localhost:11211"]
		}
	}
`)

	names := []string{"testcache1", "testcache2", "testcache3"}
	for _, name := range names {
		err := mgr.CreateCache(&cache.Config{Name: name, ProviderName: "memcache1"})
		assert.Nil(t, err, "unable to create cache")

		c := mgr.Cache(name)
		assert.NotNil(t, c)
		assert.Equal(t, name, c.Name())

		for i := 0; i < 20; i++ {
			c.Put(fmt.Sprintf("key_%v", i), i, 3*time.Second)
		}

		for i := 5; i < 10; i++ {
			v := c.Get(fmt.Sprintf("key_%v", i))
			assert.Equal(t, i, v)
		}
		c.Flush()

		p := mgr.Provider("memcache1").(*Provider)
		assert.NotNil(t, p.Client())
	}
}

func TestRedisSlideEvictionMode(t *testing.T) {
	c := createTestCache(t, "memcache1", `
	cache {
		memcache1 {
			provider = "memcache"
			# addresses = ["localhost:11211"]
		}
	}
`, &cache.Config{Name: "addgetcache", ProviderName: "memcache1", EvictionMode: cache.EvictionModeSlide})

	for i := 0; i < 20; i++ {
		c.Put(fmt.Sprintf("key_%v", i), i, 3*time.Second)
	}

	for i := 5; i < 10; i++ {
		v, err := c.GetOrPut(fmt.Sprintf("key_%v", i), i, 3*time.Second)
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}

	assert.Equal(t, "addgetcache", c.Name())
}

func TestMemcacheInvalidProviderName(t *testing.T) {
	mgr := cache.NewManager()
	mgr.AddProvider("memcache1", new(Provider))

	cfg, _ := config.ParseString(`
	cache {
		memcache1 {
			provider = "mymemcache"
			addresses = ["localhost:11211"]
		}
	}
`)
	l, _ := log.New(config.NewEmpty())
	err := mgr.InitProviders(cfg, l)
	assert.Equal(t, errors.New("aah/cache: not a vaild provider name, expected 'memcache'"), err)
}

func TestMemcacheInvalidAddress(t *testing.T) {
	mgr := cache.NewManager()
	mgr.AddProvider("memcache1", new(Provider))

	cfg, _ := config.ParseString(`
	cache {
		memcache1 {
			provider = "memcache"
			addresses = ["localhost:1123211"]
		}
	}
`)
	l, _ := log.New(config.NewEmpty())
	err := mgr.InitProviders(cfg, l)
	assert.Equal(t, errors.New("aah/cache/memcache1: memcache: no servers configured or available"), err)
}

func TestParseTimeDuration(t *testing.T) {
	d := parseDuration("", "1m")
	assert.Equal(t, float64(1), d.Minutes())
}

func createCacheMgr(t *testing.T, name, appCfgStr string) *cache.Manager {
	mgr := cache.NewManager()
	mgr.AddProvider(name, new(Provider))

	cfg, _ := config.ParseString(appCfgStr)
	l, _ := log.New(config.NewEmpty())
	l.SetWriter(ioutil.Discard)
	err := mgr.InitProviders(cfg, l)
	assert.Nil(t, err, "unexpected")
	return mgr
}

func createTestCache(t *testing.T, name, appCfgStr string, cacheCfg *cache.Config) cache.Cache {
	mgr := createCacheMgr(t, name, appCfgStr)
	e := mgr.CreateCache(cacheCfg)
	assert.Nil(t, e, "unable to create cache")
	return mgr.Cache(cacheCfg.Name)
}
